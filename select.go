package squeel

import (
	"github.com/xwb1989/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
)

/*
parseSelect processes SQL SELECT expressions and converts them into MongoDB
projection and aggregation configurations. It handles various types of SELECT
expressions including columns, functions, and subqueries.

Parameters:
- q: The Query object to modify
- node: The SELECT expressions to process

Returns:
- The modified Query object with projection and/or aggregation stages configured
*/
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

/*
selectState maintains the state of SELECT expression processing, tracking
various flags that influence how the query is built.
*/
type selectState struct {
	query           *Query // The Query object being built
	hasSubquery     bool   // Whether the SELECT contains a subquery
	hasComplexAggr  bool   // Whether complex aggregation is needed
	needsProjection bool   // Whether a projection needs to be built
}

/*
handleSelectExpr processes a single SELECT expression, handling special cases
like * expressions and converting the expression into MongoDB query components.

Parameters:
- state: The current select processing state
- expr: The SELECT expression to process

Returns:
- true if processing should continue, false if it should stop
*/
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

/*
handleAliasedSelectExpr processes an aliased SELECT expression, handling
different types of expressions including columns, functions, and subqueries.

Parameters:
- state: The current select processing state
- expr: The aliased expression to process

Returns:
- true if processing should continue, false if it should stop
*/
func (statement *Statement) handleAliasedSelectExpr(state *selectState, expr *sqlparser.AliasedExpr) bool {
	switch exprType := expr.Expr.(type) {
	case *sqlparser.CaseExpr:
		// CASE expressions require aggregation pipeline
		state.query.Operation = "aggregate"
		state.hasComplexAggr = true
		state.query.Pipeline = append(state.query.Pipeline, bson.D{{
			Key: "$project",
			Value: bson.D{{
				Key:   expr.As.String(),
				Value: statement.parseCaseExpr(state.query, exprType),
			}},
		}})
	case *sqlparser.AndExpr, *sqlparser.OrExpr:
		// Logical expressions in SELECT require aggregation
		state.query.Operation = "aggregate"
		state.hasComplexAggr = true
		state.query.Pipeline = append(state.query.Pipeline, bson.D{{
			Key: "$project",
			Value: bson.D{{
				Key:   expr.As.String(),
				Value: statement.parseExpr(state.query, exprType),
			}},
		}})
	case *sqlparser.ComparisonExpr:
		// Comparison expressions in SELECT require aggregation
		state.query.Operation = "aggregate"
		state.hasComplexAggr = true
		state.query.Pipeline = append(state.query.Pipeline, bson.D{{
			Key: "$project",
			Value: bson.D{{
				Key:   expr.As.String(),
				Value: statement.parseComparisonExpr(state.query, exprType),
			}},
		}})
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

/*
handleFuncExpr processes function expressions in SELECT clauses, converting
them into appropriate MongoDB aggregation operations.

Parameters:
- state: The current select processing state
- aliased: The aliased expression containing the function
- expr: The function expression to process
*/
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

/*
handleDistinct processes DISTINCT function expressions, configuring the query
for distinct value selection.

Parameters:
- state: The current select processing state
- expr: The DISTINCT function expression to process
*/
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

/*
getColumnFromAliasedExpr extracts a column name from an aliased expression
if it contains one.

Parameters:
- expr: The expression to extract from

Returns:
- The column name if found, nil otherwise
*/
func (statement *Statement) getColumnFromAliasedExpr(expr sqlparser.SelectExpr) *sqlparser.ColName {
	if aliased, ok := expr.(*sqlparser.AliasedExpr); ok {
		if col, ok := aliased.Expr.(*sqlparser.ColName); ok {
			return col
		}
	}
	return nil
}

/*
handleSubquery processes subquery expressions in SELECT clauses, recursively
building the subquery and incorporating it into the main query's pipeline.

Parameters:
- state: The current select processing state
- aliased: The aliased expression containing the subquery
- subquery: The subquery to process
*/
func (statement *Statement) handleSubquery(state *selectState, aliased *sqlparser.AliasedExpr, subquery *sqlparser.Subquery) {
	state.hasSubquery = true
	state.hasComplexAggr = true

	subQ := NewQuery()
	subStmt := &Statement{raw: sqlparser.String(subquery.Select)}
	if subQ, err := subStmt.Build(subQ); err == nil {
		statement.appendSubqueryPipeline(state.query, subQ, aliased.As.String())
	}
}

/*
appendSubqueryPipeline adds the necessary stages to incorporate a subquery
into the main query's pipeline using $lookup and $addFields stages.

Parameters:
- q: The main Query object
- subQ: The subquery Query object
- alias: The alias for the subquery results
*/
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

/*
finalizeSelectQuery performs final adjustments to the query based on the
presence of subqueries and complex aggregations.

Parameters:
- state: The current select processing state

Returns:
- The finalized Query object
*/
func (statement *Statement) finalizeSelectQuery(state *selectState) *Query {
	if state.hasSubquery || state.hasComplexAggr {
		state.query.Operation = "aggregate"
	}

	if len(state.query.Projection) == 0 && state.query.Operation != "aggregate" {
		state.query.Projection = nil
	}

	return state.query
}

/*
addAggregateStage adds a new aggregation stage to the query pipeline for
processing aggregate functions.

Parameters:
- state: The current select processing state
- aliased: The aliased expression containing the aggregate
- expr: The function expression
- operator: The MongoDB aggregation operator to use
- value: The value or field reference for the aggregation
*/
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

/*
handleCount processes COUNT function expressions, handling both regular and
DISTINCT COUNT operations.

Parameters:
- state: The current select processing state
- aliased: The aliased expression containing the COUNT
- expr: The COUNT function expression
*/
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

/*
handleSimpleAggregate processes simple aggregate functions (SUM, AVG, MIN, MAX),
adding appropriate aggregation stages to the pipeline.

Parameters:
- state: The current select processing state
- aliased: The aliased expression containing the aggregate
- expr: The aggregate function expression
- funcName: The name of the aggregate function
*/
func (statement *Statement) handleSimpleAggregate(state *selectState, aliased *sqlparser.AliasedExpr, expr *sqlparser.FuncExpr, funcName string) {
	if len(expr.Exprs) > 0 {
		if colExpr := statement.getColumnFromAliasedExpr(expr.Exprs[0]); colExpr != nil {
			field := colExpr.Name.CompliantName()
			statement.addAggregateStage(state, aliased, expr, "$"+funcName, "$"+field)
		}
	}
}

/*
addDistinctCountStage adds the necessary stages to perform a COUNT DISTINCT
operation using MongoDB's aggregation pipeline.

Parameters:
- state: The current select processing state
- alias: The alias for the count result
- colExpr: The column to count distinct values from
*/
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

/*
getAggregateAlias determines the appropriate alias for an aggregate function
result, using either an explicit alias or generating one based on the function
and field names.

Parameters:
- aliased: The aliased expression containing the aggregate
- expr: The aggregate function expression
- defaultName: The default name to use if no better option is available

Returns:
- The determined alias string
*/
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
