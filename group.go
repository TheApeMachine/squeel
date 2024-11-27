package squeel

import (
	"strconv"

	"github.com/xwb1989/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
)

/*
parseGroupBy processes SQL GROUP BY clauses and their associated HAVING conditions,
converting them into MongoDB aggregation pipeline stages. It creates appropriate
$group stages and handles any HAVING clause filters.

Parameters:
- q: The Query object to modify
- groupBy: The GROUP BY clauses to process
- having: The HAVING clause conditions to apply after grouping

Returns:
- The modified Query object with grouping stages configured
*/
func (statement *Statement) parseGroupBy(q *Query, groupBy sqlparser.GroupBy, having *sqlparser.Where) *Query {
	if len(groupBy) == 0 {
		return q
	}

	q.Operation = "aggregate"
	groupStage := buildGroupStage(groupBy)
	q.Pipeline = append(q.Pipeline, bson.D{{Key: mongoGroupStage, Value: groupStage}})

	return statement.addHavingClause(q, having)
}

/*
buildGroupStage creates a MongoDB $group stage from SQL GROUP BY expressions.
It sets up the grouping fields and maintains the original field values in
the output documents.

Parameters:
- groupBy: The GROUP BY expressions to process

Returns:
- A bson.M document representing the $group stage configuration
*/
func buildGroupStage(groupBy sqlparser.GroupBy) bson.M {
	groupStage := bson.M{
		"_id": bson.M{},
	}

	for _, expr := range groupBy {
		if colName, ok := expr.(*sqlparser.ColName); ok {
			field := colName.Name.CompliantName()
			groupStage["_id"].(bson.M)[field] = "$" + field
			groupStage[field] = bson.M{"$first": "$" + field}
		}
	}

	return groupStage
}

/*
addHavingClause processes the HAVING clause conditions and adds them as a $match
stage after the grouping operation. This allows filtering of grouped results
based on aggregate values.

Parameters:
- q: The Query object to modify
- having: The HAVING clause conditions to process

Returns:
- The modified Query object with HAVING conditions applied
*/
func (statement *Statement) addHavingClause(q *Query, having *sqlparser.Where) *Query {
	if having == nil || having.Expr == nil {
		return q
	}

	if matchStage := statement.parseHavingExpr(having.Expr); matchStage != nil {
		q.Pipeline = append(q.Pipeline, bson.D{{Key: "$match", Value: matchStage}})
	}

	return q
}

/*
parseHavingExpr processes a single expression from the HAVING clause and
converts it into a MongoDB match condition. Currently handles comparison
expressions.

Parameters:
- expr: The HAVING clause expression to process

Returns:
- A bson.M document representing the match condition, or nil if not applicable
*/
func (statement *Statement) parseHavingExpr(expr sqlparser.Expr) bson.M {
	if compExpr, ok := expr.(*sqlparser.ComparisonExpr); ok {
		return statement.parseHavingComparison(compExpr)
	}
	return nil
}

/*
parseHavingComparison processes a comparison expression from the HAVING clause,
converting it into a MongoDB comparison operator. It validates the operator
and handles value conversion.

Parameters:
- expr: The comparison expression to process

Returns:
- A bson.M document representing the comparison condition, or nil if invalid
*/
func (statement *Statement) parseHavingComparison(expr *sqlparser.ComparisonExpr) bson.M {
	if !isValidOperator(expr.Operator) {
		return nil
	}

	val, ok := expr.Right.(*sqlparser.SQLVal)
	if !ok {
		return nil
	}

	field := statement.getComparisonField(expr.Left)
	if field == "" {
		return nil
	}

	return bson.M{
		field: bson.M{
			mongoOperator(expr.Operator): statement.parseValue(val),
		},
	}
}

/*
getComparisonField extracts the field name from a HAVING clause expression,
handling both simple column references and function calls.

Parameters:
- left: The left-hand side of the comparison

Returns:
- The field name to use in the MongoDB comparison, or empty string if invalid
*/
func (statement *Statement) getComparisonField(left sqlparser.Expr) string {
	switch left := left.(type) {
	case *sqlparser.ColName:
		return left.Name.CompliantName()
	case *sqlparser.FuncExpr:
		return statement.getFuncField(left)
	}
	return ""
}

/*
getFuncField determines the appropriate field name for a function expression
in a HAVING clause. It handles both aliased and non-aliased function calls.

Parameters:
- funcExpr: The function expression to process

Returns:
- The field name to use in the MongoDB comparison
*/
func (statement *Statement) getFuncField(funcExpr *sqlparser.FuncExpr) string {
	if len(funcExpr.Exprs) == 0 {
		return funcExpr.Name.Lowered()
	}

	aliasedExpr, ok := funcExpr.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return funcExpr.Name.Lowered()
	}

	if aliasedExpr.As.String() != "" {
		return aliasedExpr.As.String()
	}

	if colName, ok := aliasedExpr.Expr.(*sqlparser.ColName); ok {
		return funcExpr.Name.Lowered() + "_" + colName.Name.CompliantName()
	}

	return funcExpr.Name.Lowered()
}

/*
isValidOperator checks if a comparison operator is supported in HAVING clauses.
Supported operators include standard comparison operators.

Parameters:
- op: The operator to validate

Returns:
- true if the operator is supported, false otherwise
*/
func isValidOperator(op string) bool {
	switch op {
	case ">", ">=", "<", "<=", "=", "!=":
		return true
	}
	return false
}

/*
parseValue converts a SQL value into its appropriate Go/MongoDB type.
Currently handles numeric values with special care.

Parameters:
- val: The SQL value to parse

Returns:
- The parsed value in its appropriate Go type
*/
func (statement *Statement) parseValue(val *sqlparser.SQLVal) interface{} {
	switch val.Type {
	case sqlparser.IntVal:
		if i, err := strconv.ParseInt(string(val.Val), 10, 64); err == nil {
			return i
		}
	case sqlparser.FloatVal:
		if f, err := strconv.ParseFloat(string(val.Val), 64); err == nil {
			return f
		}
	case sqlparser.StrVal:
		return string(val.Val)
	}
	return nil
}

/*
mongoOperator converts a SQL comparison operator to its MongoDB equivalent.
It maps standard SQL comparison operators to MongoDB's $gt, $gte, etc.

Parameters:
- sqlOp: The SQL operator to convert

Returns:
- The equivalent MongoDB operator
*/
func mongoOperator(sqlOp string) string {
	switch sqlOp {
	case ">":
		return "$gt"
	case ">=":
		return "$gte"
	case "<":
		return "$lt"
	case "<=":
		return "$lte"
	case "=":
		return "$eq"
	case "!=":
		return "$ne"
	default:
		return "$eq"
	}
}
