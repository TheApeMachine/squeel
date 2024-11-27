package squeel

import (
	"github.com/xwb1989/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
)

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

func (statement *Statement) buildAggregatePipelineSort(q *Query, orderBy sqlparser.OrderBy) *Query {
	sortStage := buildSortStage(orderBy)
	if len(sortStage) > 0 {
		q.Operation = "aggregate"
		q.Pipeline = append(q.Pipeline, bson.D{{Key: "$sort", Value: sortStage}})
	}
	return q
}

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
