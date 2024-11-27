package squeel

import (
	"github.com/xwb1989/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
)

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
