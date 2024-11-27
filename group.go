package squeel

import (
	"strconv"

	"github.com/xwb1989/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
)

func (statement *Statement) parseGroupBy(q *Query, groupBy sqlparser.GroupBy, having *sqlparser.Where) *Query {
	if len(groupBy) == 0 {
		return q
	}

	q.Operation = "aggregate"
	groupStage := buildGroupStage(groupBy)
	q.Pipeline = append(q.Pipeline, bson.D{{Key: mongoGroupStage, Value: groupStage}})

	return statement.addHavingClause(q, having)
}

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

func (statement *Statement) addHavingClause(q *Query, having *sqlparser.Where) *Query {
	if having == nil || having.Expr == nil {
		return q
	}

	if matchStage := statement.parseHavingExpr(having.Expr); matchStage != nil {
		q.Pipeline = append(q.Pipeline, bson.D{{Key: "$match", Value: matchStage}})
	}

	return q
}

func (statement *Statement) parseHavingExpr(expr sqlparser.Expr) bson.M {
	if compExpr, ok := expr.(*sqlparser.ComparisonExpr); ok {
		return statement.parseHavingComparison(compExpr)
	}
	return nil
}

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

func (statement *Statement) getComparisonField(left sqlparser.Expr) string {
	switch left := left.(type) {
	case *sqlparser.ColName:
		return left.Name.CompliantName()
	case *sqlparser.FuncExpr:
		return statement.getFuncField(left)
	}
	return ""
}

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

func isValidOperator(op string) bool {
	switch op {
	case ">", ">=", "<", "<=", "=", "!=":
		return true
	}
	return false
}

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
