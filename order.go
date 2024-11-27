package squeel

import (
	"github.com/xwb1989/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
)

/*
parseOrderBy converts SQL ORDER BY clauses into MongoDB sort operations.
For simple queries, it creates a sort document that can be used with find operations.
For more complex queries requiring aggregation, it adds a $sort stage to the pipeline.

Parameters:
- q: The Query object to modify
- orderBy: The SQL ORDER BY clauses to process

Returns:
- The modified Query object with sorting configuration applied
*/
func (statement *Statement) parseOrderBy(q *Query, orderBy sqlparser.OrderBy) *Query {
	if len(orderBy) == 0 {
		return q
	}

	if q.Operation != "aggregate" {
		if sortDoc := buildSimpleSort(orderBy); len(sortDoc) > 0 {
			q.Sort = sortDoc
			return q
		}
	}

	return statement.buildAggregatePipelineSort(q, orderBy)
}

/*
buildSimpleSort creates a MongoDB sort document from SQL ORDER BY clauses.
It handles basic sorting cases where each clause is a simple column reference
with an optional ASC/DESC direction.

Parameters:
- orderBy: The SQL ORDER BY clauses to convert

Returns:
- A bson.D document containing MongoDB sort specifications
*/
func buildSimpleSort(orderBy sqlparser.OrderBy) bson.D {
	sortDoc := make(bson.D, 0, len(orderBy))
	for _, order := range orderBy {
		if colName, ok := order.Expr.(*sqlparser.ColName); ok {
			direction := 1
			if order.Direction == sqlparser.DescScr {
				direction = -1
			}
			sortDoc = append(sortDoc, bson.E{
				Key:   colName.Name.CompliantName(),
				Value: direction,
			})
		}
	}
	return sortDoc
}

/*
buildAggregatePipelineSort adds a $sort stage to an aggregation pipeline based on
SQL ORDER BY clauses. This is used when the query requires aggregation operations
or when dealing with complex sorting scenarios.

Parameters:
- q: The Query object to modify
- orderBy: The SQL ORDER BY clauses to process

Returns:
- The modified Query object with a $sort stage added to its pipeline
*/
func (statement *Statement) buildAggregatePipelineSort(q *Query, orderBy sqlparser.OrderBy) *Query {
	sortStage := buildSortStage(orderBy)
	if len(sortStage) > 0 {
		q.Operation = "aggregate"
		q.Pipeline = append(q.Pipeline, bson.D{{Key: "$sort", Value: sortStage}})
	}
	return q
}

/*
buildSortStage creates a MongoDB sort stage document from SQL ORDER BY clauses.
It converts each ORDER BY clause into a field-direction pair in the format
expected by MongoDB's $sort operator.

Parameters:
- orderBy: The SQL ORDER BY clauses to convert

Returns:
- A bson.M document containing the sort stage configuration
*/
func buildSortStage(orderBy sqlparser.OrderBy) bson.M {
	sortStage := bson.M{}
	for _, order := range orderBy {
		if colName, ok := order.Expr.(*sqlparser.ColName); ok {
			direction := 1
			if order.Direction == sqlparser.DescScr {
				direction = -1
			}
			sortStage[colName.Name.CompliantName()] = direction
		}
	}
	return sortStage
}
