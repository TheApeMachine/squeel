package squeel

import (
	"github.com/xwb1989/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
)

const mongoGroupStage = "$group"

/*
parseFunc processes SQL function expressions and converts them into appropriate
MongoDB aggregation operations. It handles various SQL functions including COUNT,
MIN, MAX, SUM, and AVG, converting them into equivalent MongoDB operations.

The function determines whether to use a simple count operation or a more complex
aggregation pipeline based on the presence of DISTINCT and aliases.

Parameters:
- q: The Query object to modify
- node: The function expression to process

Returns:
- The modified Query object with the function operation configured
*/
func (statement *Statement) parseFunc(q *Query, node *sqlparser.FuncExpr) *Query {
	if node == nil {
		return q
	}

	switch node.Name.Lowered() {
	case "count":
		// Always use aggregate for DISTINCT counts or when there's an alias
		if node.Distinct {
			q.Operation = "aggregate"
			return handleCountStar(q, node)
		}

		// Check if this COUNT is aliased in the parent expression
		if parent, ok := statement.getParentExpr(node); ok && parent.As.String() != "" {
			q.Operation = "aggregate"
			return handleCountStar(q, node)
		}

		// For simple COUNT cases
		if len(node.Exprs) <= 1 {
			if len(node.Exprs) == 0 || isStarExpr(node.Exprs[0]) || sqlparser.String(node.Exprs[0]) == "q.*" {
				q.Operation = "count"
				return q
			}
		}

		// For all other cases, use aggregate
		q.Operation = "aggregate"
		return handleRegularCount(q, node)

	case "min", "max", "sum", "avg":
		q.Operation = "aggregate"
		return handleAggregateFunc(q, node)
	}

	return q
}

/*
isStarExpr checks if a SELECT expression is a star (*) expression.
This is used to identify COUNT(*) type queries.

Parameters:
- expr: The SELECT expression to check

Returns:
- true if the expression is a star expression, false otherwise
*/
func isStarExpr(expr sqlparser.SelectExpr) bool {
	if aliasedExpr, ok := expr.(*sqlparser.AliasedExpr); ok {
		// Check if it's a star expression by checking the string representation
		return sqlparser.String(aliasedExpr.Expr) == "*"
	}
	return false
}

/*
handleCountStar processes COUNT(*) and COUNT(DISTINCT ...) expressions,
converting them into appropriate MongoDB aggregation stages.

Parameters:
- q: The Query object to modify
- node: The COUNT function expression to process

Returns:
- The modified Query object with the count aggregation configured
*/
func handleCountStar(q *Query, node *sqlparser.FuncExpr) *Query {
	q.Operation = "aggregate"
	alias := getAlias(node)
	groupStage := bson.M{
		"_id": nil,
		alias: bson.M{"$sum": 1},
	}
	q.Pipeline = append(q.Pipeline, bson.D{{Key: mongoGroupStage, Value: groupStage}})
	return q
}

/*
handleRegularCount processes regular COUNT(column) expressions, converting
them into MongoDB aggregation pipelines that count non-null values.

Parameters:
- q: The Query object to modify
- node: The COUNT function expression to process

Returns:
- The modified Query object with the count aggregation configured
*/
func handleRegularCount(q *Query, node *sqlparser.FuncExpr) *Query {
	q.Operation = "aggregate"
	if len(node.Exprs) == 0 {
		return q
	}

	alias := getAlias(node)
	if aliasedExpr, ok := node.Exprs[0].(*sqlparser.AliasedExpr); ok {
		if colName, ok := aliasedExpr.Expr.(*sqlparser.ColName); ok {
			field := colName.Name.CompliantName()
			groupStage := bson.M{
				"_id": nil,
				alias: bson.M{"$sum": bson.M{
					"$cond": []interface{}{
						bson.M{"$ne": []interface{}{"$" + field, nil}},
						1,
						0,
					},
				}},
			}
			q.Pipeline = append(q.Pipeline, bson.D{{Key: mongoGroupStage, Value: groupStage}})
		}
	}
	return q
}

/*
handleAggregateFunc processes aggregate functions (MIN, MAX, SUM, AVG),
converting them into appropriate MongoDB aggregation stages.

Parameters:
- q: The Query object to modify
- node: The aggregate function expression to process

Returns:
- The modified Query object with the aggregation configured
*/
func handleAggregateFunc(q *Query, node *sqlparser.FuncExpr) *Query {
	q.Operation = "aggregate"
	if len(node.Exprs) == 0 {
		return q
	}

	alias := getAlias(node)
	if aliasedExpr, ok := node.Exprs[0].(*sqlparser.AliasedExpr); ok {
		if colName, ok := aliasedExpr.Expr.(*sqlparser.ColName); ok {
			field := colName.Name.CompliantName()
			groupStage := bson.M{
				"_id": nil,
				alias: bson.M{"$" + node.Name.Lowered(): "$" + field},
			}
			q.Pipeline = append(q.Pipeline, bson.D{{Key: mongoGroupStage, Value: groupStage}})
		}
	}
	return q
}

/*
getAlias determines the appropriate alias for an aggregate function result.
It checks for explicit aliases in the expression, and if none is found,
generates one based on the function name and field name.

Parameters:
- node: The function expression to get an alias for

Returns:
- The determined alias string
*/
func getAlias(node *sqlparser.FuncExpr) string {
	// Check if the first expression has an alias
	if len(node.Exprs) > 0 {
		if aliasedExpr, ok := node.Exprs[0].(*sqlparser.AliasedExpr); ok {
			if aliasedExpr.As.String() != "" {
				return aliasedExpr.As.String()
			}
			if colName, ok := aliasedExpr.Expr.(*sqlparser.ColName); ok {
				return node.Name.Lowered() + "_" + colName.Name.CompliantName()
			}
		}
	}
	return node.Name.Lowered()
}

/*
getParentExpr attempts to find the parent AliasedExpr for a given FuncExpr
by walking the SQL AST. This is used to determine if a function has an alias
in its parent context.

Parameters:
- node: The function expression to find the parent for

Returns:
- The parent AliasedExpr if found, and a boolean indicating success
*/
func (statement *Statement) getParentExpr(node *sqlparser.FuncExpr) (*sqlparser.AliasedExpr, bool) {
	if statement.stmt == nil {
		return nil, false
	}

	var parent *sqlparser.AliasedExpr
	found := false

	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (kontinue bool, err error) {
		if aliased, ok := n.(*sqlparser.AliasedExpr); ok {
			if funcExpr, ok := aliased.Expr.(*sqlparser.FuncExpr); ok {
				if funcExpr == node {
					parent = aliased
					found = true
					return false, nil
				}
			}
		}
		return true, nil
	}, statement.stmt)

	return parent, found
}
