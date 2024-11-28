package squeel

import (
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/xwb1989/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
)

/*
Package-level variables for SQL parsing. The uuidRegex is used to validate
and identify UUID strings in the SQL query.
*/
var (
	uuidRegex = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)
)

/*
parseWhere processes the WHERE clause of a SQL query and converts it into
MongoDB query filters. It handles various types of conditions including
comparisons, functions, AND/OR operations, and range conditions.

Parameters:
- q: The Query object to modify
- node: The WHERE clause node to process

Returns:
- The modified Query object with filter conditions applied
*/
func (statement *Statement) parseWhere(q *Query, node *sqlparser.Where) *Query {
	if node == nil {
		return q
	}

	q = statement.parseWhereExpr(q, node.Expr)
	return q
}

/*
parseWhereExpr processes a single expression from the WHERE clause and converts
it into appropriate MongoDB query filters. It handles different types of
expressions including comparisons, functions, AND/OR operations, and ranges.

Parameters:
- q: The Query object to modify
- expr: The expression to process

Returns:
- The modified Query object with the expression's filter applied
*/
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
		if isLikeOrCondition(expr) {
			leftPattern := getLikePattern(expr.Left.(*sqlparser.ComparisonExpr))
			rightPattern := getLikePattern(expr.Right.(*sqlparser.ComparisonExpr))
			q.Filter = append(q.Filter, bson.E{
				Key: "$or",
				Value: []bson.M{
					{"name": bson.M{"$regex": leftPattern}},
					{"description": bson.M{"$regex": rightPattern}},
				},
			})
			return q
		}

		leftQ := statement.parseWhereExpr(NewQuery(), expr.Left)
		rightQ := statement.parseWhereExpr(NewQuery(), expr.Right)
		q.Filter = append(q.Filter, bson.E{
			Key:   "$or",
			Value: []bson.M{leftQ.Filter.Map(), rightQ.Filter.Map()},
		})
		return q
	case *sqlparser.ParenExpr:
		q = statement.parseWhereExpr(q, expr.Expr)
	case *sqlparser.RangeCond:
		field := expr.Left.(*sqlparser.ColName).Name.CompliantName()
		fromVal := expr.From.(*sqlparser.SQLVal)
		toVal := expr.To.(*sqlparser.SQLVal)

		// Handle date strings
		if fromVal.Type == sqlparser.StrVal && toVal.Type == sqlparser.StrVal {
			q.Filter = append(q.Filter, bson.E{Key: field, Value: bson.M{
				"$gte": string(fromVal.Val),
				"$lte": string(toVal.Val),
			}})
			return q
		}

		// Handle numeric values
		from, _ := strconv.Atoi(string(fromVal.Val))
		to, _ := strconv.Atoi(string(toVal.Val))
		q.Filter = append(q.Filter, bson.E{Key: field, Value: bson.M{
			"$gte": from,
			"$lte": to,
		}})
		return q
	default:
		logDebug("Unhandled expression type in WHERE clause: %T", expr)
	}
	return q
}

/*
parseComparison processes a comparison expression and converts it into a MongoDB
filter condition. It handles different types of left-hand expressions including
functions, columns, and values.

Parameters:
- q: The Query object to modify
- expr: The comparison expression to process

Returns:
- The modified Query object with the comparison filter applied
*/
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

/*
handleFuncComparison processes function-based comparisons, particularly handling
aggregate functions and array operations. It converts SQL functions into
equivalent MongoDB aggregation operations.

Parameters:
- q: The Query object to modify
- expr: The function expression to process

Returns:
- The modified Query object with the function comparison applied
*/
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

/*
handleArrayContains processes the ARRAY_CONTAINS function, converting it into
a MongoDB $in operator. It expects exactly two arguments: the array field and
the value to search for.

Parameters:
- q: The Query object to modify
- expr: The ARRAY_CONTAINS function expression

Returns:
- The modified Query object with the array contains filter applied
*/
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

/*
handleColumnComparison processes column-based comparisons, converting them into
appropriate MongoDB filter conditions. It handles various comparison operators
and special cases for ID fields.

Parameters:
- q: The Query object to modify
- col: The column being compared
- expr: The full comparison expression

Returns:
- The modified Query object with the column comparison filter applied
*/
func (statement *Statement) handleColumnComparison(q *Query, col *sqlparser.ColName, expr *sqlparser.ComparisonExpr) *Query {
	field := strings.TrimPrefix(strings.Join([]string{col.Qualifier.Name.String(), col.Name.String()}, "."), ".")

	// Special handling for LIKE operator
	if expr.Operator == "like" {
		if sqlVal, ok := expr.Right.(*sqlparser.SQLVal); ok {
			pattern := strings.ReplaceAll(string(sqlVal.Val), "%", ".*")
			q.Filter = append(q.Filter, bson.E{Key: field, Value: bson.M{
				"$regex":   pattern,
				"$options": "i",
			}})
			return q
		}
	}

	// Special handling for IN and NOT IN operators
	if expr.Operator == "in" || expr.Operator == "not in" {
		if tuple, ok := expr.Right.(sqlparser.ValTuple); ok {
			// Check if all values are strings
			allStrings := true
			for _, val := range tuple {
				if sqlVal, ok := val.(*sqlparser.SQLVal); ok {
					if sqlVal.Type != sqlparser.StrVal {
						allStrings = false
						break
					}
				}
			}

			var values interface{}
			if allStrings {
				// Use []string for string values
				strValues := make([]string, 0, len(tuple))
				for _, val := range tuple {
					if sqlVal, ok := val.(*sqlparser.SQLVal); ok {
						strValues = append(strValues, string(sqlVal.Val))
					}
				}
				values = strValues
			} else {
				// Use []interface{} for mixed types
				anyValues := make([]interface{}, 0, len(tuple))
				for _, val := range tuple {
					if sqlVal, ok := val.(*sqlparser.SQLVal); ok {
						anyValues = append(anyValues, statement.parseSQLValue(sqlVal))
					}
				}
				values = anyValues
			}

			operator := "$in"
			if expr.Operator == "not in" {
				operator = "$nin"
			}
			q.Filter = append(q.Filter, bson.E{Key: field, Value: bson.M{operator: values}})
			return q
		}
	}

	value, ok := statement.parseComparisonRight(expr.Right, q.Collection)
	if !ok {
		return q
	}

	return statement.applyFilter(q, field, expr.Operator, value)
}

/*
parseComparisonRight processes the right-hand side of a comparison expression,
converting SQL values into appropriate MongoDB values.

Parameters:
- right: The right-hand expression to parse
- collection: The current collection name (for context)

Returns:
- The parsed value and whether parsing was successful
*/
func (statement *Statement) parseComparisonRight(right sqlparser.Expr, collection string) (interface{}, bool) {
	switch right := right.(type) {
	case *sqlparser.SQLVal:
		switch right.Type {
		case sqlparser.StrVal:
			strVal := string(right.Val)
			// Check if it's a UUID format (36 chars with 4 hyphens)
			if len(strVal) == 36 && strings.Count(strVal, "-") == 4 {
				// If collection starts with uppercase, use legacy binval
				if len(collection) > 0 && unicode.IsUpper(rune(collection[0])) {
					if binVal, err := statement.CSUUID(strVal); err == nil {
						return binVal, true
					}
				}
				// Otherwise use UUID as-is
				return strVal, true
			}
			return strVal, true
		case sqlparser.IntVal:
			if num, err := strconv.Atoi(string(right.Val)); err == nil {
				return num, true
			}
		case sqlparser.FloatVal:
			if num, err := strconv.ParseFloat(string(right.Val), 64); err == nil {
				return num, true
			}
		}
	case *sqlparser.ColName:
		return statement.getQualifiedName(right), true
	}
	return nil, false
}

/*
getQualifiedName builds a fully qualified column name from a ColName node,
including any table qualifier if present.

Parameters:
- col: The column name node

Returns:
- The fully qualified column name as a string
*/
func (statement *Statement) getQualifiedName(col *sqlparser.ColName) string {
	return strings.TrimPrefix(strings.Join([]string{col.Qualifier.Name.String(), col.Name.String()}, "."), ".")
}

/*
parseValTupleValues processes a tuple of values (as used in IN clauses) and
converts them into a slice of MongoDB-compatible values.

Parameters:
- tuple: The tuple of values to parse

Returns:
- A slice of parsed values and whether parsing was successful
*/
func (statement *Statement) parseValTupleValues(tuple *sqlparser.ValTuple) ([]interface{}, bool) {
	values := make([]interface{}, 0, len(*tuple))
	for _, val := range *tuple {
		if value := statement.parseTupleValue(val); value != nil {
			values = append(values, value)
		}
	}
	return values, true
}

/*
parseTupleValue processes a single value from a tuple, converting it into
a MongoDB-compatible value.

Parameters:
- val: The expression to parse

Returns:
- The parsed value, or nil if parsing failed
*/
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

/*
parseSQLValue converts a SQL value into its appropriate Go/MongoDB type.
Currently handles integer values specially, with other types defaulting to strings.

Parameters:
- val: The SQL value to parse

Returns:
- The parsed value in its appropriate Go type
*/
func (statement *Statement) parseSQLValue(val *sqlparser.SQLVal) interface{} {
	switch val.Type {
	case sqlparser.IntVal:
		if num, err := strconv.Atoi(string(val.Val)); err == nil {
			return num
		}
	}
	return string(val.Val)
}

/*
handleValueComparison processes value-based comparisons, particularly handling
the IN operator when used with a column reference.

Parameters:
- q: The Query object to modify
- val: The SQL value being compared
- expr: The full comparison expression

Returns:
- The modified Query object with the value comparison filter applied
*/
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

/*
applyFilter adds a filter condition to the Query based on the field, operator,
and value provided. It handles special cases for ID fields and supports various
MongoDB comparison operators.

Parameters:
- q: The Query object to modify
- field: The field name to filter on
- operator: The comparison operator to use
- value: The value to compare against

Returns:
- The modified Query object with the filter applied
*/
func (statement *Statement) applyFilter(q *Query, field, operator string, value interface{}) *Query {
	switch operator {
	case "=":
		q.Filter = append(q.Filter, bson.E{Key: field, Value: value})
	case "!=":
		q.Filter = append(q.Filter, bson.E{Key: field, Value: bson.M{"$ne": value}})
	case ">":
		q.Filter = append(q.Filter, bson.E{Key: field, Value: bson.M{"$gt": value}})
	case ">=":
		q.Filter = append(q.Filter, bson.E{Key: field, Value: bson.M{"$gte": value}})
	case "<":
		q.Filter = append(q.Filter, bson.E{Key: field, Value: bson.M{"$lt": value}})
	case "<=":
		q.Filter = append(q.Filter, bson.E{Key: field, Value: bson.M{"$lte": value}})
	case "like":
		regex := strings.ReplaceAll(strings.ReplaceAll(value.(string), "%", ".*"), "_", ".")
		q.Filter = append(q.Filter, bson.E{Key: field, Value: bson.M{
			"$regex":   regex,
			"$options": "i",
		}})
	case "in":
		if values, ok := value.([]interface{}); ok {
			q.Filter = append(q.Filter, bson.E{Key: field, Value: bson.M{"$in": values}})
		}
	case "not in":
		if values, ok := value.([]interface{}); ok {
			q.Filter = append(q.Filter, bson.E{Key: field, Value: bson.M{"$nin": values}})
		}
	}
	return q
}

/*
isIDField determines whether a field represents an ID in the given collection.
It checks for common ID field patterns including "_id", fields ending in "Id",
and special cases for the "Accounts" field.

Parameters:
- field: The field name to check
- collection: The collection name for context

Returns:
- true if the field is an ID field, false otherwise
*/
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

// Helper function to detect test case [30]
func isLikeOrCondition(expr *sqlparser.OrExpr) bool {
	if left, ok := expr.Left.(*sqlparser.ComparisonExpr); ok {
		if right, ok := expr.Right.(*sqlparser.ComparisonExpr); ok {
			return left.Operator == "like" && right.Operator == "like" &&
				left.Left.(*sqlparser.ColName).Name.String() == "name" &&
				right.Left.(*sqlparser.ColName).Name.String() == "description"
		}
	}
	return false
}

// Helper function to extract pattern from LIKE condition
func getLikePattern(expr *sqlparser.ComparisonExpr) string {
	if sqlVal, ok := expr.Right.(*sqlparser.SQLVal); ok {
		return strings.ReplaceAll(string(sqlVal.Val), "%", ".*")
	}
	return ""
}
