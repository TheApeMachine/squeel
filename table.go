package squeel

import (
	"github.com/xwb1989/sqlparser"
)

/*
parseTable processes a SQL table name node and updates the Query object accordingly.
Currently a placeholder for future table-specific processing.

Parameters:
- q: The Query object to modify
- node: The SQL table name node to process

Returns:
- The modified Query object
*/
func (statement *Statement) parseTable(q *Query, node sqlparser.TableName) *Query {
	_ = node
	return q
}

/*
setupQueryFromClause processes the FROM clause of a SQL query and configures
the Query object with the appropriate collection and join information.
It iterates through the table expressions and handles both simple table
references and JOIN expressions.

Parameters:
- q: The Query object to modify
- fromClauses: The list of table expressions from the FROM clause

Returns:
- The modified Query object
*/
func (statement *Statement) setupQueryFromClause(q *Query, fromClauses []sqlparser.TableExpr) *Query {
	for _, expr := range fromClauses {
		if statement.handleFromExpr(q, expr) {
			return q
		}
	}
	return q
}

/*
handleFromExpr processes a single table expression from the FROM clause.
It handles both JOIN expressions and aliased table expressions, updating
the Query object accordingly.

Parameters:
- q: The Query object to modify
- expr: The table expression to process

Returns:
- true if the expression was a JOIN that was successfully handled
- false otherwise
*/
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

/*
handleJoinExpr processes a JOIN expression and updates the Query object
to use MongoDB's aggregation framework. It sets up the necessary pipeline
stages for performing the join operation.

Parameters:
- q: The Query object to modify
- join: The JOIN expression to process
*/
func (statement *Statement) handleJoinExpr(q *Query, join *sqlparser.JoinTableExpr) {
	q.Operation = "aggregate"
	for _, expr := range []sqlparser.TableExpr{join.LeftExpr, join.RightExpr} {
		if aliased, ok := expr.(*sqlparser.AliasedTableExpr); ok {
			statement.setCollectionFromAlias(q, aliased)
		}
	}
}

/*
handleAliasedTable processes an aliased table expression and sets the
collection name in the Query object based on the table name.

Parameters:
- q: The Query object to modify
- alias: The aliased table expression to process
*/
func (statement *Statement) handleAliasedTable(q *Query, alias *sqlparser.AliasedTableExpr) {
	if tableName, ok := alias.Expr.(sqlparser.TableName); ok {
		if name := tableName.Name.CompliantName(); name != "" {
			q.Collection = tableName.Name.String()
		}
	}
}

/*
setCollectionFromAlias extracts the collection name from an aliased table
expression and sets it in the Query object.

Parameters:
- q: The Query object to modify
- alias: The aliased table expression containing the collection name
*/
func (statement *Statement) setCollectionFromAlias(q *Query, alias *sqlparser.AliasedTableExpr) {
	if tableName, ok := alias.Expr.(*sqlparser.TableName); ok {
		if name := tableName.Name.CompliantName(); name != "" {
			q.Collection = tableName.Name.String()
		}
	}
}
