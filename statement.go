package squeel

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	errnie "github.com/theapemachine/errnie/v3"
	"github.com/xwb1989/sqlparser"
	"go.mongodb.org/mongo-driver/bson"
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
		case sqlparser.Comments:
			// Comments are ignored
			return true, nil
		case sqlparser.ColIdent:
			// Column identifiers are handled by their parent nodes
			return true, nil
		case sqlparser.TableIdent:
			// Table identifiers are handled by their parent nodes
			return true, nil
		case *sqlparser.ColName:
			// Column names are handled by their parent nodes
			return true, nil
		case *sqlparser.SQLVal:
			// SQL values are handled by their parent nodes
			return true, nil
		case sqlparser.ValTuple:
			// Value tuples are handled by their parent nodes
			return true, nil
		case sqlparser.Exprs:
			// Expression lists are handled by their parent nodes
			return true, nil
		case *sqlparser.AliasedTableExpr:
			// Handle aliased table expressions
			if node.Expr != nil {
				return true, nil
			}
		case *sqlparser.IndexHints:
			// Index hints are not applicable to MongoDB
			return true, nil
		case *sqlparser.StarExpr:
			// Star expressions are handled by their parent nodes
			return true, nil
		case sqlparser.JoinCondition:
			// Join conditions are handled by parseJoin
			return true, nil
		case *sqlparser.ComparisonExpr:
			// Comparison expressions are handled by parseWhere
			return true, nil
		case *sqlparser.AndExpr:
			// AND expressions are handled by parseWhere
			return true, nil
		case *sqlparser.OrExpr:
			// OR expressions are handled by parseWhere
			return true, nil
		case *sqlparser.ParenExpr:
			// Parenthesized expressions are handled by their parent nodes
			return true, nil
		case *sqlparser.When:
			// WHEN clauses are handled by their parent nodes
			return true, nil
		case sqlparser.GroupBy:
			// Group by clauses are handled by parseGroupBy
			return true, nil
		case sqlparser.OrderBy:
			// Order by clauses are handled by parseOrderBy
			return true, nil
		case *sqlparser.Order:
			// Order specifications are handled by parseOrderBy
			return true, nil
		case sqlparser.Columns:
			// Column lists are handled by their parent nodes
			return true, nil
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

/*
parseCaseExpr converts a SQL CASE expression into MongoDB's $cond operator.
It processes both simple CASE expressions and searched CASE expressions.

Parameters:
- q: The Query object to modify
- node: The CASE expression node to process

Returns:
- A bson.D representing the MongoDB $cond operator
*/
func (statement *Statement) parseCaseExpr(q *Query, node *sqlparser.CaseExpr) bson.D {
	if node == nil {
		return nil
	}

	// CASE expressions require aggregation pipeline
	q.Operation = "aggregate"

	// Handle searched CASE expression (CASE WHEN ... THEN ...)
	if node.Expr == nil {
		return statement.parseSearchedCase(q, node)
	}

	// Handle simple CASE expression (CASE expr WHEN ... THEN ...)
	return statement.parseSimpleCase(q, node)
}

/*
parseSearchedCase handles CASE expressions in the form:
CASE WHEN condition THEN result [WHEN condition THEN result]... [ELSE result] END

Parameters:
- q: The Query object to modify
- node: The CASE expression node to process

Returns:
- A bson.D representing the MongoDB $cond operator chain
*/
func (statement *Statement) parseSearchedCase(q *Query, node *sqlparser.CaseExpr) bson.D {
	if len(node.Whens) == 0 {
		return nil
	}

	// Handle a single WHEN clause
	if len(node.Whens) == 1 {
		when := node.Whens[0]
		return bson.D{bson.E{
			Key: "$cond",
			Value: bson.D{
				bson.E{Key: "if", Value: statement.parseExpr(q, when.Cond)},
				bson.E{Key: "then", Value: statement.parseExpr(q, when.Val)},
				bson.E{Key: "else", Value: statement.parseExpr(q, node.Else)},
			},
		}}
	}

	// Handle multiple WHEN clauses by nesting $cond operators
	return statement.parseNestedCase(q, node.Whens, node.Else)
}

/*
parseSimpleCase handles CASE expressions in the form:
CASE expr WHEN value THEN result [WHEN value THEN result]... [ELSE result] END

Parameters:
- q: The Query object to modify
- node: The CASE expression node to process

Returns:
- A bson.D representing the MongoDB $cond operator chain
*/
func (statement *Statement) parseSimpleCase(q *Query, node *sqlparser.CaseExpr) bson.D {
	if len(node.Whens) == 0 {
		return nil
	}

	// Convert simple CASE to searched CASE by comparing expr with WHEN values
	whens := make([]*sqlparser.When, len(node.Whens))
	for i, when := range node.Whens {
		// Create a comparison between case expression and when value
		comparison := &sqlparser.ComparisonExpr{
			Operator: sqlparser.EqualStr,
			Left:     node.Expr,
			Right:    when.Cond,
		}
		whens[i] = &sqlparser.When{
			Cond: comparison,
			Val:  when.Val,
		}
	}

	return statement.parseNestedCase(q, whens, node.Else)
}

/*
parseNestedCase handles multiple WHEN clauses by nesting $cond operators.

Parameters:
- q: The Query object to modify
- whens: The list of WHEN clauses
- elseExpr: The ELSE expression

Returns:
- A bson.D representing the nested MongoDB $cond operators
*/
func (statement *Statement) parseNestedCase(q *Query, whens []*sqlparser.When, elseExpr sqlparser.Expr) bson.D {
	if len(whens) == 0 {
		if elseExpr == nil {
			return nil
		}
		return bson.D{bson.E{Key: "$literal", Value: statement.parseExpr(q, elseExpr)}}
	}

	// Process the last WHEN clause
	lastWhen := whens[len(whens)-1]

	// Base case: last WHEN clause
	if len(whens) == 1 {
		return bson.D{bson.E{
			Key: "$cond",
			Value: bson.D{
				bson.E{Key: "if", Value: statement.parseExpr(q, lastWhen.Cond)},
				bson.E{Key: "then", Value: statement.parseExpr(q, lastWhen.Val)},
				bson.E{Key: "else", Value: statement.parseExpr(q, elseExpr)},
			},
		}}
	}

	// Recursive case: nest the remaining WHEN clauses
	return bson.D{bson.E{
		Key: "$cond",
		Value: bson.D{
			bson.E{Key: "if", Value: statement.parseExpr(q, lastWhen.Cond)},
			bson.E{Key: "then", Value: statement.parseExpr(q, lastWhen.Val)},
			bson.E{Key: "else", Value: statement.parseNestedCase(q, whens[:len(whens)-1], elseExpr)},
		},
	}}
}

/*
parseExpr converts a SQL expression into its MongoDB equivalent.
This is a general-purpose expression parser that handles various SQL expression types.

Parameters:
- q: The Query object to modify
- expr: The SQL expression to parse

Returns:
- The MongoDB equivalent of the expression
*/
func (statement *Statement) parseExpr(q *Query, expr sqlparser.Expr) interface{} {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *sqlparser.ColName:
		return "$" + expr.Name.String()

	case *sqlparser.SQLVal:
		return statement.parseSQLVal(expr)

	case *sqlparser.FuncExpr:
		return statement.parseFuncExprToMongo(q, expr)

	case *sqlparser.CaseExpr:
		return statement.parseCaseExpr(q, expr)

	case *sqlparser.ComparisonExpr:
		return statement.parseComparisonExpr(q, expr)

	case *sqlparser.AndExpr:
		return bson.D{bson.E{
			Key: "$and",
			Value: []interface{}{
				statement.parseExpr(q, expr.Left),
				statement.parseExpr(q, expr.Right),
			},
		}}

	case *sqlparser.OrExpr:
		return bson.D{bson.E{
			Key: "$or",
			Value: []interface{}{
				statement.parseExpr(q, expr.Left),
				statement.parseExpr(q, expr.Right),
			},
		}}

	case *sqlparser.ParenExpr:
		return statement.parseExpr(q, expr.Expr)

	case sqlparser.ValTuple:
		values := make([]interface{}, len(expr))
		for i, val := range expr {
			values[i] = statement.parseExpr(q, val)
		}
		return values

	default:
		// Log unhandled expression type
		errnie.Error(fmt.Errorf("unhandled expression type: %T", expr))
		return nil
	}
}

/*
parseSQLVal converts a SQL value into its native Go/MongoDB equivalent.

Parameters:
- node: The SQL value node to convert

Returns:
- The native Go value
*/
func (statement *Statement) parseSQLVal(node *sqlparser.SQLVal) interface{} {
	switch node.Type {
	case sqlparser.StrVal:
		return string(node.Val)
	case sqlparser.IntVal:
		val, _ := strconv.ParseInt(string(node.Val), 10, 64)
		return val
	case sqlparser.FloatVal:
		val, _ := strconv.ParseFloat(string(node.Val), 64)
		return val
	case sqlparser.HexNum:
		val, _ := strconv.ParseInt(string(node.Val), 16, 64)
		return val
	case sqlparser.HexVal:
		return node.Val
	case sqlparser.BitVal:
		val, _ := strconv.ParseInt(string(node.Val), 2, 64)
		return val
	default:
		return string(node.Val)
	}
}

/*
parseFuncExprToMongo converts a SQL function call to its MongoDB aggregation equivalent.

Parameters:
- q: The Query object to modify
- node: The function expression to convert

Returns:
- The MongoDB aggregation operator
*/
func (statement *Statement) parseFuncExprToMongo(q *Query, node *sqlparser.FuncExpr) interface{} {
	name := node.Name.String()

	// Helper to get the expression from a SelectExpr
	getExpr := func(expr sqlparser.SelectExpr) sqlparser.Expr {
		if aliased, ok := expr.(*sqlparser.AliasedExpr); ok {
			return aliased.Expr
		}
		return nil
	}

	// Helper to get the first expression from Exprs
	getFirstExpr := func() sqlparser.Expr {
		if len(node.Exprs) > 0 {
			return getExpr(node.Exprs[0])
		}
		return nil
	}

	switch strings.ToUpper(name) {
	case "COUNT":
		if node.Distinct {
			if expr := getFirstExpr(); expr != nil {
				return bson.D{bson.E{Key: "$addToSet", Value: statement.parseExpr(q, expr)}}
			}
		}
		return bson.D{bson.E{Key: "$sum", Value: 1}}

	case "SUM":
		if expr := getFirstExpr(); expr != nil {
			return bson.D{bson.E{Key: "$sum", Value: statement.parseExpr(q, expr)}}
		}

	case "AVG":
		if expr := getFirstExpr(); expr != nil {
			return bson.D{bson.E{Key: "$avg", Value: statement.parseExpr(q, expr)}}
		}

	case "MIN":
		if expr := getFirstExpr(); expr != nil {
			return bson.D{bson.E{Key: "$min", Value: statement.parseExpr(q, expr)}}
		}

	case "MAX":
		if expr := getFirstExpr(); expr != nil {
			return bson.D{bson.E{Key: "$max", Value: statement.parseExpr(q, expr)}}
		}
	}

	errnie.Error(fmt.Errorf("unhandled function: %s", name))
	return nil
}

/*
parseComparisonExpr converts a SQL comparison expression to its MongoDB equivalent.

Parameters:
- q: The Query object to modify
- node: The comparison expression to convert

Returns:
- The MongoDB comparison operator
*/
func (statement *Statement) parseComparisonExpr(q *Query, node *sqlparser.ComparisonExpr) interface{} {
	// We only need the right value for MongoDB operators
	right := statement.parseExpr(q, node.Right)

	switch node.Operator {
	case sqlparser.EqualStr:
		return right
	case sqlparser.NotEqualStr:
		return bson.M{"$ne": right}
	case sqlparser.GreaterThanStr:
		return bson.M{"$gt": right}
	case sqlparser.GreaterEqualStr:
		return bson.M{"$gte": right}
	case sqlparser.LessThanStr:
		return bson.M{"$lt": right}
	case sqlparser.LessEqualStr:
		return bson.M{"$lte": right}
	case sqlparser.InStr:
		// For IN operator, right should be a slice of values
		if values, ok := right.([]interface{}); ok {
			return bson.M{"$in": values}
		}
		return nil
	case sqlparser.NotInStr:
		// For NOT IN operator, right should be a slice of values
		if values, ok := right.([]interface{}); ok {
			return bson.M{"$nin": values}
		}
		return nil
	case sqlparser.LikeStr:
		if str, ok := right.(string); ok {
			// Convert SQL LIKE pattern to MongoDB regex
			pattern := strings.ReplaceAll(str, "%", ".*")
			pattern = strings.ReplaceAll(pattern, "_", ".")
			return bson.M{"$regex": pattern, "$options": "i"}
		}
		return nil
	default:
		errnie.Error(fmt.Errorf("unhandled comparison operator: %s", node.Operator))
		return nil
	}
}
