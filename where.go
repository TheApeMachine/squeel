package squeel

import (
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/xwb1989/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	uuidRegex = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)
)

func (statement *Statement) parseWhere(q *Query, node *sqlparser.Where) *Query {
	if node == nil {
		return q
	}

	q = statement.parseWhereExpr(q, node.Expr)
	return q
}

func (statement *Statement) parseWhereExpr(q *Query, expr sqlparser.Expr) *Query {
	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		q = statement.parseComparison(q, expr)
	case *sqlparser.FuncExpr:
		q = statement.parseComparison(q, &sqlparser.ComparisonExpr{
			Left:     expr,
			Operator: "=",
			Right:    sqlparser.NewStrVal([]byte("true")),
		})
	case *sqlparser.AndExpr:
		q = statement.parseWhereExpr(q, expr.Left)
		q = statement.parseWhereExpr(q, expr.Right)
	case *sqlparser.OrExpr:
		leftQ := statement.parseWhereExpr(NewQuery(), expr.Left)
		rightQ := statement.parseWhereExpr(NewQuery(), expr.Right)
		q.Filter = append(q.Filter, bson.E{Key: "$or", Value: []bson.M{leftQ.Filter.Map(), rightQ.Filter.Map()}})
	case *sqlparser.ParenExpr:
		q = statement.parseWhereExpr(q, expr.Expr)
	case *sqlparser.RangeCond:
		field := expr.Left.(*sqlparser.ColName).Name.CompliantName()
		from, _ := strconv.Atoi(string(expr.From.(*sqlparser.SQLVal).Val))
		to, _ := strconv.Atoi(string(expr.To.(*sqlparser.SQLVal).Val))
		q.Filter = append(q.Filter, bson.E{Key: field, Value: bson.M{"$gte": from, "$lte": to}})
	default:
		logDebug("Unhandled expression type in WHERE clause: %T", expr)
	}
	return q
}

func (statement *Statement) parseComparison(q *Query, expr *sqlparser.ComparisonExpr) *Query {
	switch left := expr.Left.(type) {
	case *sqlparser.FuncExpr:
		return statement.handleFuncComparison(q, left)
	case *sqlparser.ColName:
		return statement.handleColumnComparison(q, left, expr)
	case *sqlparser.SQLVal:
		return statement.handleValueComparison(q, left, expr)
	default:
		logDebug("Unhandled left expression type in comparison: %T with value %v", left, left)
	}
	return q
}

func (statement *Statement) handleFuncComparison(q *Query, expr *sqlparser.FuncExpr) *Query {
	switch expr.Name.Lowered() {
	case "array_contains":
		return statement.handleArrayContains(q, expr)
	case "count", "avg", "sum", "min", "max":
		// These are aggregate functions - they should be handled in HAVING clause
		q.Operation = "aggregate"
		if len(expr.Exprs) > 0 {
			if colExpr := statement.getColumnFromAliasedExpr(expr.Exprs[0]); colExpr != nil {
				field := colExpr.Name.CompliantName()
				operator := "$" + expr.Name.Lowered()
				if expr.Name.Lowered() == "count" {
					operator = "$sum"
					q.Pipeline = append(q.Pipeline, bson.D{{
						Key: "$group",
						Value: bson.D{
							{Key: "_id", Value: nil},
							{Key: field, Value: bson.M{operator: 1}},
						},
					}})
				} else {
					q.Pipeline = append(q.Pipeline, bson.D{{
						Key: "$group",
						Value: bson.D{
							{Key: "_id", Value: nil},
							{Key: field, Value: bson.M{operator: "$" + field}},
						},
					}})
				}
			}
		}
		return q
	default:
		logDebug("where.handleFuncComparison: Unhandled function: %s", expr.Name.String())
		return q
	}
}

func (statement *Statement) handleArrayContains(q *Query, expr *sqlparser.FuncExpr) *Query {
	if len(expr.Exprs) != 2 {
		logDebug("ARRAY_CONTAINS requires 2 arguments, but got %d", len(expr.Exprs))
		return q
	}

	field := expr.Exprs[0].(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Name.CompliantName()
	value := expr.Exprs[1].(*sqlparser.AliasedExpr).Expr.(*sqlparser.SQLVal).Val

	parsedVal, err := statement.parseID(string(value), q.Collection)
	if err != nil {
		logDebug("Error parsing ID for ARRAY_CONTAINS: %v", err)
		return q
	}

	q.Filter = append(q.Filter, bson.E{Key: field, Value: bson.M{"$in": []interface{}{parsedVal}}})
	q.Projection = nil
	return q
}

func (statement *Statement) handleColumnComparison(q *Query, col *sqlparser.ColName, expr *sqlparser.ComparisonExpr) *Query {
	field := strings.TrimPrefix(strings.Join([]string{col.Qualifier.Name.String(), col.Name.String()}, "."), ".")
	value, ok := statement.parseComparisonRight(expr.Right, q.Collection)
	if !ok {
		return q
	}

	return statement.applyFilter(q, field, expr.Operator, value)
}

func (statement *Statement) parseComparisonRight(right sqlparser.Expr, _ string) (interface{}, bool) {
	switch right := right.(type) {
	case *sqlparser.SQLVal:
		return string(right.Val), true
	case *sqlparser.ColName:
		return statement.getQualifiedName(right), true
	case *sqlparser.ValTuple:
		return statement.parseValTupleValues(right)
	}
	return nil, false
}

func (statement *Statement) getQualifiedName(col *sqlparser.ColName) string {
	return strings.TrimPrefix(strings.Join([]string{col.Qualifier.Name.String(), col.Name.String()}, "."), ".")
}

func (statement *Statement) parseValTupleValues(tuple *sqlparser.ValTuple) ([]interface{}, bool) {
	values := make([]interface{}, 0, len(*tuple))
	for _, val := range *tuple {
		if value := statement.parseTupleValue(val); value != nil {
			values = append(values, value)
		}
	}
	return values, true
}

func (statement *Statement) parseTupleValue(val sqlparser.Expr) interface{} {
	switch v := val.(type) {
	case *sqlparser.SQLVal:
		return statement.parseSQLValue(v)
	case *sqlparser.ColName:
		return statement.getQualifiedName(v)
	default:
		return nil
	}
}

func (statement *Statement) parseSQLValue(val *sqlparser.SQLVal) interface{} {
	switch val.Type {
	case sqlparser.IntVal:
		if num, err := strconv.Atoi(string(val.Val)); err == nil {
			return num
		}
	}
	return string(val.Val)
}

func (statement *Statement) handleValueComparison(q *Query, val *sqlparser.SQLVal, expr *sqlparser.ComparisonExpr) *Query {
	if expr.Operator != "in" {
		return q
	}

	colName, ok := expr.Right.(*sqlparser.ColName)
	if !ok {
		return q
	}

	field := colName.Name.CompliantName()
	parsedVal, err := statement.parseID(string(val.Val), q.Collection)
	if err != nil {
		logDebug("Error parsing ID for IN clause: %v", err)
		return q
	}

	q.Filter = append(q.Filter, bson.E{Key: field, Value: bson.M{"$in": []interface{}{parsedVal}}})
	return q
}

func (statement *Statement) applyFilter(q *Query, field, operator string, value interface{}) *Query {
	var filter bson.E

	if isIDField(field, q.Collection) {
		parsedVal, err := statement.parseID(value.(string), q.Collection)
		if err != nil {
			logDebug("Error parsing ID: %v", err)
			return q
		}
		value = parsedVal
	}

	switch operator {
	case "=":
		filter = bson.E{Key: field, Value: value}
	case "!=":
		filter = bson.E{Key: field, Value: bson.M{"$ne": value}}
	case ">":
		filter = bson.E{Key: field, Value: bson.M{"$gt": value}}
	case ">=":
		filter = bson.E{Key: field, Value: bson.M{"$gte": value}}
	case "<":
		filter = bson.E{Key: field, Value: bson.M{"$lt": value}}
	case "<=":
		filter = bson.E{Key: field, Value: bson.M{"$lte": value}}
	case "like":
		regex := strings.ReplaceAll(strings.ReplaceAll(value.(string), "%", ".*"), "_", ".")
		filter = bson.E{Key: field, Value: bson.M{"$regex": regex, "$options": "i"}}
	case "in":
		if values, ok := value.([]interface{}); ok {
			filter = bson.E{Key: field, Value: bson.M{"$in": values}}
		}
	default:
		logDebug("Unhandled operator in comparison: %s", operator)
		return q
	}

	q.Filter = append(q.Filter, filter)
	return q
}

func isIDField(field string, collection string) bool {
	return field == "_id" ||
		strings.HasSuffix(field, "Id") ||
		(field == "Accounts" && unicode.IsUpper(rune(collection[0])))
}

func (statement *Statement) parseID(value string, collection string) (interface{}, error) {
	// Check if it's a UUID
	if uuidRegex.MatchString(value) {
		// If the collection name starts with an uppercase letter, convert to Binary
		if unicode.IsUpper(rune(collection[0])) {
			return statement.CSUUID(value) // Use the same CSUUID function as in the test
		}
		// Otherwise, return as string
		return value, nil
	}

	// For non-UUID values, return as string
	return value, nil
}

func Map(d bson.D) bson.M {
	m := bson.M{}
	for _, e := range d {
		m[e.Key] = e.Value
	}
	return m
}
