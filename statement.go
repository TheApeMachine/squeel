package squeel

import (
	"fmt"
	"reflect"

	errnie "github.com/theapemachine/errnie/v3"
	"github.com/xwb1989/sqlparser"
)

/*
Statement represents a SQL statement that can be parsed and converted into
a MongoDB query. It maintains both the raw SQL string and the parsed AST
representation of the statement.
*/
type Statement struct {
	raw  string              // The original SQL query string
	stmt sqlparser.Statement // The parsed SQL statement AST
	err  error               // Any error that occurred during parsing
}

/*
NewStatement creates a new Statement instance from a raw SQL query string.
It initializes the statement with the raw SQL but does not parse it until
Build is called.

Parameters:
- raw: The SQL query string to be parsed

Returns:
- A new Statement instance ready for building
*/
func NewStatement(raw string) *Statement {
	return &Statement{raw: raw}
}

/*
Build processes the SQL statement and constructs a MongoDB query configuration.
It first validates the statement, then parses the SQL and walks through the
AST to build the appropriate MongoDB query components.

Parameters:
- q: The Query object to populate with MongoDB query configuration

Returns:
- The populated Query object
- Any error that occurred during building
*/
func (statement *Statement) Build(q *Query) (*Query, error) {
	if err := statement.validate(); err != nil {
		return q, err
	}

	if err := statement.parseSQL(q); err != nil {
		return q, err
	}

	return statement.finalizeQuery(q)
}

/*
validate performs basic validation on the Statement instance to ensure
it is ready for parsing. It checks for nil statement and existing errors.

Returns:
- Any validation error that occurred
*/
func (statement *Statement) validate() error {
	if statement == nil {
		return fmt.Errorf("statement is nil")
	}
	if statement.err != nil {
		return statement.err
	}
	return nil
}

/*
parseSQL parses the raw SQL string into an AST and processes it to build
the MongoDB query configuration. It uses the sqlparser library to parse
the SQL and then walks through the AST nodes to construct the query.

Parameters:
- q: The Query object to populate during parsing

Returns:
- Any error that occurred during parsing or processing
*/
func (statement *Statement) parseSQL(q *Query) error {
	var err error
	statement.stmt, err = sqlparser.Parse(statement.raw)
	if err != nil {
		return errnie.Error(err)
	}

	return errnie.Error(sqlparser.Walk(statement.walkNode(q), statement.stmt))
}

/*
walkNode returns a function that processes each node in the SQL AST.
It handles different types of SQL nodes (SELECT, WHERE, etc.) and updates
the Query object accordingly. Unhandled node types are tracked but not processed.

Parameters:
- q: The Query object to modify during AST traversal

Returns:
  - A function that processes each AST node and returns whether to continue walking
    and any error that occurred
*/
func (statement *Statement) walkNode(q *Query) func(node sqlparser.SQLNode) (bool, error) {
	unhandledTypes := make(map[string]bool)

	return func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case sqlparser.TableName:
			q = statement.parseTable(q, node)
		case *sqlparser.Select:
			q = statement.handleSelectNode(q, node)
		case sqlparser.SelectExprs:
			q = statement.parseSelect(q, node)
		case *sqlparser.Where:
			q = statement.parseWhere(q, node)
		case *sqlparser.Limit:
			q = statement.parseLimit(q, node)
		case *sqlparser.FuncExpr:
			q = statement.parseFunc(q, node)
		case *sqlparser.JoinTableExpr:
			q = statement.parseJoin(q, node)
		case *sqlparser.AliasedExpr:
			statement.handleAliasedExpr(q, node)
		case sqlparser.TableExprs:
			// No-op
		default:
			nodeType := reflect.TypeOf(node).String()
			unhandledTypes[nodeType] = true
		}
		return true, nil
	}
}

/*
handleSelectNode processes a SELECT statement node, configuring the Query
object with the appropriate MongoDB operation type and processing various
clauses like GROUP BY, HAVING, and ORDER BY.

Parameters:
- q: The Query object to modify
- node: The SELECT statement node to process

Returns:
- The modified Query object
*/
func (statement *Statement) handleSelectNode(q *Query, node *sqlparser.Select) *Query {
	if q.Collection == "" {
		statement.setupQueryFromClause(q, node.From)
	}
	if node.Distinct != "" {
		q.Operation = "distinct"
	} else if q.Operation == "" {
		q.Operation = "find"
	}
	q = statement.parseGroupBy(q, node.GroupBy, node.Having)
	return statement.parseOrderBy(q, node.OrderBy)
}

/*
handleAliasedExpr processes an aliased expression, particularly handling
subqueries by recursively building them into the aggregation pipeline.

Parameters:
- q: The Query object to modify
- node: The aliased expression node to process
*/
func (statement *Statement) handleAliasedExpr(q *Query, node *sqlparser.AliasedExpr) {
	if subquery, ok := node.Expr.(*sqlparser.Subquery); ok {
		subQ := NewQuery()
		subStmt := &Statement{raw: sqlparser.String(subquery.Select)}
		if _, err := subStmt.Build(subQ); err == nil {
			q.Pipeline = append(q.Pipeline, subQ.Pipeline...)
		}
	}
}

/*
finalizeQuery performs final adjustments to the Query object based on the
SQL statement type and its components. It determines whether the query needs
to use MongoDB's aggregation framework based on various factors.

Parameters:
- q: The Query object to finalize

Returns:
- The finalized Query object
- Any error that occurred during finalization
*/
func (statement *Statement) finalizeQuery(q *Query) (*Query, error) {
	if selectNode, ok := statement.stmt.(*sqlparser.Select); ok {
		needsAggregate := len(selectNode.GroupBy) > 0 ||
			selectNode.Having != nil ||
			len(selectNode.OrderBy) > 0 ||
			len(selectNode.From) > 1

		if needsAggregate && q.Operation != "count" {
			q.Operation = "aggregate"
		} else if q.Operation == "" {
			q.Operation = "find"
		}
	}
	return q, nil
}
