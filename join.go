package squeel

import (
	"github.com/xwb1989/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
)

/*
parseJoin processes a SQL JOIN expression and converts it into MongoDB's $lookup aggregation.
It extracts information from the JoinTableExpr node to build a lookup stage that will be
added to the query pipeline. The function handles both the left and right expressions
of the join, setting up the appropriate collection references and field mappings.

The function modifies the Query object in place, adding a $lookup stage to its pipeline
with the following components:
- from: the collection to join with
- as: the field to store the joined documents
- localField: the field from the main collection
- foreignField: the field from the joined collection
*/
func (statement *Statement) parseJoin(q *Query, node *sqlparser.JoinTableExpr) *Query {
	// Set operation type to aggregate since we're using $lookup
	q.Operation = "aggregate"

	if name := statement.aliasedTableName(node.LeftExpr); name != "" {
		q.Collection = name
	}

	rightTable := statement.aliasedTableName(node.RightExpr)
	if rightTable == "" {
		return q
	}

	// Handle the ON condition
	onExpr := statement.on(node)
	if onExpr == nil {
		return q
	}

	// Check if we have a subquery in the ON clause
	if subquery, ok := onExpr.Right.(*sqlparser.Subquery); ok {
		// Extract fields from subquery
		sel, ok := subquery.Select.(*sqlparser.Select)
		if !ok {
			return q
		}

		// Get the field we're selecting
		selectExpr, ok := sel.SelectExprs[0].(*sqlparser.AliasedExpr)
		if !ok {
			return q
		}
		selectField := statement.colName(selectExpr.Expr)

		// Get the WHERE condition
		whereExpr, ok := sel.Where.Expr.(*sqlparser.ComparisonExpr)
		if !ok {
			return q
		}
		whereField := statement.colName(whereExpr.Left)

		// Create a pipeline for the subquery
		q.Pipeline = append(q.Pipeline, bson.D{{
			Key: "$lookup",
			Value: bson.M{
				"from": sel.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String(),
				"let":  bson.M{whereField: "$" + whereField},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$eq": []string{"$" + whereField, "$$" + whereField},
							},
						},
					},
					{"$sort": bson.M{sel.OrderBy[0].Expr.(*sqlparser.ColName).Name.String(): -1}},
					{"$limit": 1},
					{"$project": bson.M{selectField: 1}},
				},
				"as": rightTable,
			},
		}})
		q.Pipeline = append(q.Pipeline, bson.D{bson.E{Key: "$unwind", Value: bson.M{
			"path":                       "$" + rightTable,
			"preserveNullAndEmptyArrays": true,
		}}})
		return q
	}

	// Handle regular JOIN
	leftField := statement.colName(onExpr.Left)
	rightField := statement.colName(onExpr.Right)
	if leftField != "" && rightField != "" {
		q.Pipeline = append(q.Pipeline, bson.D{{
			Key: "$lookup",
			Value: bson.M{
				"from":         rightTable,
				"localField":   leftField,
				"foreignField": rightField,
				"as":           rightTable,
			},
		}})
		q.Pipeline = append(q.Pipeline, bson.D{bson.E{Key: "$unwind", Value: bson.M{
			"path":                       "$" + rightTable,
			"preserveNullAndEmptyArrays": true,
		}}})
	}

	return q
}
