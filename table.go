package squeel

import (
	"github.com/xwb1989/sqlparser"
)

func (statement *Statement) parseTable(q *Query, node sqlparser.TableName) *Query {
	_ = node
	return q
}

func (statement *Statement) setupQueryFromClause(q *Query, fromClauses []sqlparser.TableExpr) *Query {
	for _, expr := range fromClauses {
		if statement.handleFromExpr(q, expr) {
			return q
		}
	}
	return q
}

func (statement *Statement) handleFromExpr(q *Query, expr sqlparser.TableExpr) bool {
	switch exprType := expr.(type) {
	case *sqlparser.JoinTableExpr:
		statement.handleJoinExpr(q, exprType)
		return true
	case *sqlparser.AliasedTableExpr:
		statement.handleAliasedTable(q, exprType)
	}
	return false
}

func (statement *Statement) handleJoinExpr(q *Query, join *sqlparser.JoinTableExpr) {
	q.Operation = "aggregate"
	for _, expr := range []sqlparser.TableExpr{join.LeftExpr, join.RightExpr} {
		if aliased, ok := expr.(*sqlparser.AliasedTableExpr); ok {
			statement.setCollectionFromAlias(q, aliased)
		}
	}
}

func (statement *Statement) handleAliasedTable(q *Query, alias *sqlparser.AliasedTableExpr) {
	if tableName, ok := alias.Expr.(sqlparser.TableName); ok {
		if name := tableName.Name.CompliantName(); name != "" {
			q.Collection = tableName.Name.String()
		}
	}
}

func (statement *Statement) setCollectionFromAlias(q *Query, alias *sqlparser.AliasedTableExpr) {
	if tableName, ok := alias.Expr.(*sqlparser.TableName); ok {
		if name := tableName.Name.CompliantName(); name != "" {
			q.Collection = tableName.Name.String()
		}
	}
}
