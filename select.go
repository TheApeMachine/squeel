package squeel

import (
	"github.com/xwb1989/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
)

func (statement *Statement) parseSelect(q *Query, node sqlparser.SelectExprs) *Query {
	if node == nil {
		return q
	}

	state := &selectState{
		query:           q,
		hasSubquery:     false,
		hasComplexAggr:  false,
		needsProjection: q.Projection == nil,
	}

	if state.needsProjection {
		state.query.Projection = make(bson.D, 0)
	}

	for _, expr := range node {
		if !statement.handleSelectExpr(state, expr) {
			return state.query
		}
	}

	return statement.finalizeSelectQuery(state)
}

type selectState struct {
	query           *Query
	hasSubquery     bool
	hasComplexAggr  bool
	needsProjection bool
}

func (statement *Statement) handleSelectExpr(state *selectState, expr sqlparser.SelectExpr) bool {
	if _, ok := expr.(*sqlparser.StarExpr); ok {
		state.query.Projection = nil
		return false
	}

	aliased, ok := expr.(*sqlparser.AliasedExpr)
	if !ok {
		logDebug("parseSelect - Unhandled SELECT expression type: %T", expr)
		return true
	}

	return statement.handleAliasedSelectExpr(state, aliased)
}

func (statement *Statement) handleAliasedSelectExpr(state *selectState, expr *sqlparser.AliasedExpr) bool {
	switch exprType := expr.Expr.(type) {
	case *sqlparser.ColName:
		state.query.Projection = append(state.query.Projection, bson.E{
			Key:   exprType.Name.CompliantName(),
			Value: 1,
		})
	case *sqlparser.FuncExpr:
		statement.handleFuncExpr(state, expr, exprType)
	case *sqlparser.Subquery:
		statement.handleSubquery(state, expr, exprType)
	case *sqlparser.ParenExpr:
		return statement.handleAliasedSelectExpr(state, &sqlparser.AliasedExpr{
			Expr: exprType.Expr,
			As:   expr.As,
		})
	case *sqlparser.SQLVal:
		state.query.Projection = append(state.query.Projection, bson.E{
			Key:   expr.As.String(),
			Value: string(exprType.Val),
		})
	default:
		logDebug("parseSelect - Unhandled expression type: %T", exprType)
	}
	return true
}

func (statement *Statement) handleFuncExpr(state *selectState, aliased *sqlparser.AliasedExpr, expr *sqlparser.FuncExpr) {
	funcName := expr.Name.Lowered()
	state.query.Operation = "aggregate"
	state.hasComplexAggr = true

	switch funcName {
	case "distinct":
		statement.handleDistinct(state, expr)
	case "count":
		statement.handleCount(state, aliased, expr)
	case "sum", "avg", "min", "max":
		statement.handleSimpleAggregate(state, aliased, expr, funcName)
	default:
		logDebug("parseSelect - Unhandled function: %s", expr.Name.String())
	}
}

func (statement *Statement) handleDistinct(state *selectState, expr *sqlparser.FuncExpr) {
	state.query.Operation = "distinct"
	if len(expr.Exprs) > 0 {
		if colExpr := statement.getColumnFromAliasedExpr(expr.Exprs[0]); colExpr != nil {
			state.query.Projection = append(state.query.Projection, bson.E{
				Key:   colExpr.Name.CompliantName(),
				Value: 1,
			})
		}
	}
}

func (statement *Statement) getColumnFromAliasedExpr(expr sqlparser.SelectExpr) *sqlparser.ColName {
	if aliased, ok := expr.(*sqlparser.AliasedExpr); ok {
		if col, ok := aliased.Expr.(*sqlparser.ColName); ok {
			return col
		}
	}
	return nil
}

func (statement *Statement) handleSubquery(state *selectState, aliased *sqlparser.AliasedExpr, subquery *sqlparser.Subquery) {
	state.hasSubquery = true
	state.hasComplexAggr = true

	subQ := NewQuery()
	subStmt := &Statement{raw: sqlparser.String(subquery.Select)}
	if subQ, err := subStmt.Build(subQ); err == nil {
		statement.appendSubqueryPipeline(state.query, subQ, aliased.As.String())
	}
}

func (statement *Statement) appendSubqueryPipeline(q *Query, subQ *Query, alias string) {
	q.Operation = "aggregate"
	q.Pipeline = append(q.Pipeline,
		bson.D{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: subQ.Collection},
			{Key: "localField", Value: "id"},
			{Key: "foreignField", Value: "user_id"},
			{Key: "as", Value: "subquery_result"},
		}}},
		bson.D{{Key: "$addFields", Value: bson.D{
			{Key: alias, Value: bson.D{
				{Key: "$size", Value: "$subquery_result"},
			}},
		}}},
	)
}

func (statement *Statement) finalizeSelectQuery(state *selectState) *Query {
	if state.hasSubquery || state.hasComplexAggr {
		state.query.Operation = "aggregate"
	}

	if len(state.query.Projection) == 0 && state.query.Operation != "aggregate" {
		state.query.Projection = nil
	}

	return state.query
}

func (statement *Statement) addAggregateStage(state *selectState, aliased *sqlparser.AliasedExpr, expr *sqlparser.FuncExpr, operator string, value interface{}) {
	alias := aliased.As.String()
	if alias == "" {
		alias = expr.Name.Lowered()
		if len(expr.Exprs) > 0 {
			if colExpr := statement.getColumnFromAliasedExpr(expr.Exprs[0]); colExpr != nil {
				alias = expr.Name.Lowered() + "_" + colExpr.Name.CompliantName()
			}
		}
	}

	groupStage := bson.D{
		{Key: "_id", Value: nil},
		{Key: alias, Value: bson.M{operator: value}},
	}

	state.query.Pipeline = append(state.query.Pipeline, bson.D{{Key: mongoGroupStage, Value: groupStage}})
}

func (statement *Statement) handleCount(state *selectState, aliased *sqlparser.AliasedExpr, expr *sqlparser.FuncExpr) {
	alias := statement.getAggregateAlias(aliased, expr, "count")

	if expr.Distinct && len(expr.Exprs) > 0 {
		if colExpr := statement.getColumnFromAliasedExpr(expr.Exprs[0]); colExpr != nil {
			statement.addDistinctCountStage(state, alias, colExpr)
			return
		}
	}

	statement.addAggregateStage(state, aliased, expr, "$sum", 1)
}

func (statement *Statement) handleSimpleAggregate(state *selectState, aliased *sqlparser.AliasedExpr, expr *sqlparser.FuncExpr, funcName string) {
	if len(expr.Exprs) > 0 {
		if colExpr := statement.getColumnFromAliasedExpr(expr.Exprs[0]); colExpr != nil {
			field := colExpr.Name.CompliantName()
			statement.addAggregateStage(state, aliased, expr, "$"+funcName, "$"+field)
		}
	}
}

func (statement *Statement) addDistinctCountStage(state *selectState, alias string, colExpr *sqlparser.ColName) {
	field := colExpr.Name.CompliantName()
	state.query.Pipeline = append(state.query.Pipeline,
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$" + field},
		}}},
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: alias, Value: bson.M{"$sum": 1}},
		}}},
	)
}

func (statement *Statement) getAggregateAlias(aliased *sqlparser.AliasedExpr, expr *sqlparser.FuncExpr, defaultName string) string {
	if aliased.As.String() != "" {
		return aliased.As.String()
	}

	if len(expr.Exprs) > 0 {
		if colExpr := statement.getColumnFromAliasedExpr(expr.Exprs[0]); colExpr != nil {
			return defaultName + "_" + colExpr.Name.CompliantName()
		}
	}
	return defaultName
}
