package squeel

import (
	"github.com/xwb1989/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
)

const mongoGroupStage = "$group"

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

func isStarExpr(expr sqlparser.SelectExpr) bool {
	if aliasedExpr, ok := expr.(*sqlparser.AliasedExpr); ok {
		// Check if it's a star expression by checking the string representation
		return sqlparser.String(aliasedExpr.Expr) == "*"
	}
	return false
}

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

// getParentExpr tries to find the parent AliasedExpr for a FuncExpr
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
