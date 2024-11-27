package squeel

import (
	"fmt"
	"reflect"

	errnie "github.com/theapemachine/errnie/v3"
	"github.com/xwb1989/sqlparser"
)

type Statement struct {
	raw  string
	stmt sqlparser.Statement
	err  error
}

func NewStatement(raw string) *Statement {
	return &Statement{raw: raw}
}

func (statement *Statement) Build(q *Query) (*Query, error) {
	if err := statement.validate(); err != nil {
		return q, err
	}

	if err := statement.parseSQL(q); err != nil {
		return q, err
	}

	return statement.finalizeQuery(q)
}

func (statement *Statement) validate() error {
	if statement == nil {
		return fmt.Errorf("statement is nil")
	}
	if statement.err != nil {
		return statement.err
	}
	return nil
}

func (statement *Statement) parseSQL(q *Query) error {
	var err error
	statement.stmt, err = sqlparser.Parse(statement.raw)
	if err != nil {
		return errnie.Error(err)
	}

	return errnie.Error(sqlparser.Walk(statement.walkNode(q), statement.stmt))
}

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

func (statement *Statement) handleAliasedExpr(q *Query, node *sqlparser.AliasedExpr) {
	if subquery, ok := node.Expr.(*sqlparser.Subquery); ok {
		subQ := NewQuery()
		subStmt := &Statement{raw: sqlparser.String(subquery.Select)}
		if _, err := subStmt.Build(subQ); err == nil {
			q.Pipeline = append(q.Pipeline, subQ.Pipeline...)
		}
	}
}

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
