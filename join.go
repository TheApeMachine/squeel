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
	if name := statement.aliasedTableName(node.LeftExpr); name != "" {
		q.Collection = name
		q.Pipeline = append(q.Pipeline, bson.D{{
			Key:   "$lookup",
			Value: bson.M{},
		}})
	}

	if name := statement.aliasedTableName(node.RightExpr); name != "" {
		q.Pipeline[0][0].Value.(bson.M)["from"] = name
		q.Pipeline[0][0].Value.(bson.M)["as"] = name
	}

	if name := statement.colName(statement.on(node).Left); name != "" {
		q.Pipeline[0][0].Value.(bson.M)["localField"] = name
	}

	if name := statement.colName(statement.on(node).Right); name != "" {
		q.Pipeline[0][0].Value.(bson.M)["localField"] = name
	}

	return q
}
