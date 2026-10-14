package squeel

import (
	"fmt"
	"strconv"
	"strings"

	errnie "github.com/theapemachine/errnie/v3"
	"github.com/xwb1989/sqlparser"
)

/*
Parser parses SQL query strings into abstract, storage-agnostic Plan structures.
It has zero dependencies on any database-specific driver (MongoDB, S3, etc.).
*/
type Parser struct{}

// NewParser creates a new Parser instance.
func NewParser() *Parser {
	return &Parser{}
}

// Parse parses a raw SQL query string into an abstract Plan.
func (p *Parser) Parse(raw string) (*Plan, error) {
	return ParseSQL(raw)
}

// ParseSQL parses a raw SQL query string into an abstract Plan.
func ParseSQL(raw string) (*Plan, error) {
	stmt, err := sqlparser.Parse(raw)
	if err != nil {
		return nil, errnie.Error(err)
	}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, errnie.Error(fmt.Errorf("unsupported SQL statement type: %T", stmt))
	}

	plan := NewPlan(raw)

	// 1. Parse FROM clause (tables and joins)
	if err := parseFrom(plan, selectStmt.From); err != nil {
		return nil, errnie.Error(err)
	}

	// 2. Parse DISTINCT
	if selectStmt.Distinct != "" {
		plan.Distinct = true
	}

	// 3. Parse SELECT expressions (projections, aggregates, subqueries)
	if err := parseSelectExprs(plan, selectStmt.SelectExprs); err != nil {
		return nil, errnie.Error(err)
	}

	// 4. Parse WHERE clause
	if selectStmt.Where != nil && selectStmt.Where.Expr != nil {
		plan.Where = parseWhereExpr(selectStmt.Where.Expr)
	}

	// 5. Parse GROUP BY clause
	parseGroupBy(plan, selectStmt.GroupBy)

	// 6. Parse HAVING clause
	if selectStmt.Having != nil && selectStmt.Having.Expr != nil {
		plan.Having = parseWhereExpr(selectStmt.Having.Expr)
	}

	// 7. Parse ORDER BY clause
	parseOrderBy(plan, selectStmt.OrderBy)

	// 8. Parse LIMIT and OFFSET
	parseLimit(plan, selectStmt.Limit)

	return plan, nil
}

func parseFrom(plan *Plan, fromClauses sqlparser.TableExprs) error {
	for _, expr := range fromClauses {
		if err := parseTableExpr(plan, expr); err != nil {
			return err
		}
	}
	return nil
}

func parseTableExpr(plan *Plan, expr sqlparser.TableExpr) error {
	switch t := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tableName, ok := t.Expr.(sqlparser.TableName); ok {
			tbl := TableRef{
				Name:  tableName.Name.String(),
				Alias: t.As.String(),
			}
			if plan.Table.Name == "" {
				plan.Table = tbl
			}
		}
	case *sqlparser.JoinTableExpr:
		// Left table
		if err := parseTableExpr(plan, t.LeftExpr); err != nil {
			return err
		}

		// Right table
		var rightTable TableRef
		if right, ok := t.RightExpr.(*sqlparser.AliasedTableExpr); ok {
			if tableName, ok := right.Expr.(sqlparser.TableName); ok {
				rightTable = TableRef{
					Name:  tableName.Name.String(),
					Alias: right.As.String(),
				}
			}
		}

		// ON condition
		var onExpr Expr
		var leftField, rightField string
		if t.Condition.On != nil {
			onExpr = parseWhereExpr(t.Condition.On)
			if comp, ok := t.Condition.On.(*sqlparser.ComparisonExpr); ok {
				if col, ok := comp.Left.(*sqlparser.ColName); ok {
					leftField = col.Name.CompliantName()
				}
				if col, ok := comp.Right.(*sqlparser.ColName); ok {
					rightField = col.Name.CompliantName()
				}
			}
		}

		joinType := strings.ToUpper(t.Join)
		if joinType == "" {
			joinType = "JOIN"
		}

		plan.Joins = append(plan.Joins, Join{
			Type:       joinType,
			Table:      rightTable,
			On:         onExpr,
			LeftField:  leftField,
			RightField: rightField,
		})
	}
	return nil
}

func parseSelectExprs(plan *Plan, selectExprs sqlparser.SelectExprs) error {
	for _, expr := range selectExprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			plan.Projections = append(plan.Projections, Projection{
				Expr:   &StarExpr{},
				IsStar: true,
			})
		case *sqlparser.AliasedExpr:
			proj, err := parseAliasedSelectExpr(e)
			if err != nil {
				return err
			}
			if proj.Distinct {
				plan.Distinct = true
			}
			plan.Projections = append(plan.Projections, proj)
		}
	}
	return nil
}

func parseAliasedSelectExpr(aliased *sqlparser.AliasedExpr) (Projection, error) {
	alias := aliased.As.String()

	switch expr := aliased.Expr.(type) {

	case *sqlparser.ColName:
		return Projection{
			Expr: &ColumnExpr{
				Qualifier: expr.Qualifier.Name.String(),
				Name:      expr.Name.CompliantName(),
			},
			Alias: alias,
		}, nil

	case *sqlparser.FuncExpr:
		fExpr := parseFuncExpr(expr)
		isDistinct := strings.EqualFold(expr.Name.Lowered(), "distinct")
		return Projection{
			Expr:     fExpr,
			Alias:    alias,
			Distinct: isDistinct,
		}, nil

	case *sqlparser.Subquery:
		subPlan, err := ParseSQL(sqlparser.String(expr.Select))
		if err != nil {
			return Projection{}, err
		}
		return Projection{
			Expr:  &SubqueryExpr{Plan: subPlan},
			Alias: alias,
		}, nil

	case *sqlparser.ParenExpr:
		return parseAliasedSelectExpr(&sqlparser.AliasedExpr{
			Expr: expr.Expr,
			As:   aliased.As,
		})

	case *sqlparser.SQLVal:
		return Projection{
			Expr:  &LiteralExpr{Value: parseSQLValue(expr)},
			Alias: alias,
		}, nil

	default:
		// Check for star expressions like COUNT(*) or COUNT(q.*)
		strVal := sqlparser.String(expr)
		if strVal == "*" || strVal == "q.*" {
			qualifier := ""
			if strVal == "q.*" {
				qualifier = "q"
			}
			return Projection{
				Expr:   &StarExpr{Qualifier: qualifier},
				Alias:  alias,
				IsStar: true,
			}, nil
		}
		return Projection{
			Expr:  &LiteralExpr{Value: strVal},
			Alias: alias,
		}, nil
	}
}

func parseFuncExpr(funcExpr *sqlparser.FuncExpr) *FuncExpr {
	name := funcExpr.Name.Lowered()
	args := make([]Expr, 0, len(funcExpr.Exprs))

	for _, arg := range funcExpr.Exprs {
		if aliasedArg, ok := arg.(*sqlparser.AliasedExpr); ok {
			str := sqlparser.String(aliasedArg.Expr)
			if str == "*" || str == "q.*" {
				qualifier := ""
				if str == "q.*" {
					qualifier = "q"
				}
				args = append(args, &StarExpr{Qualifier: qualifier})
				continue
			}
			args = append(args, parseExpr(aliasedArg.Expr))
		}
	}

	return &FuncExpr{
		Name:     name,
		Args:     args,
		Distinct: funcExpr.Distinct,
	}
}

func parseWhereExpr(expr sqlparser.Expr) Expr {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		left := parseExpr(e.Left)
		right := parseExpr(e.Right)
		return &BinaryExpr{
			Left:     left,
			Operator: strings.ToUpper(e.Operator),
			Right:    right,
		}

	case *sqlparser.AndExpr:
		left := parseWhereExpr(e.Left)
		right := parseWhereExpr(e.Right)
		return &LogicalExpr{
			Operator: "AND",
			Exprs:    []Expr{left, right},
		}

	case *sqlparser.OrExpr:
		left := parseWhereExpr(e.Left)
		right := parseWhereExpr(e.Right)
		return &LogicalExpr{
			Operator: "OR",
			Exprs:    []Expr{left, right},
		}

	case *sqlparser.ParenExpr:
		return parseWhereExpr(e.Expr)

	case *sqlparser.RangeCond:
		field := ""
		if col, ok := e.Left.(*sqlparser.ColName); ok {
			field = col.Name.CompliantName()
		}
		from := parseExprValue(e.From)
		to := parseExprValue(e.To)
		return &RangeExpr{
			Field: field,
			From:  from,
			To:    to,
		}

	case *sqlparser.FuncExpr:
		return parseFuncExpr(e)

	default:
		return parseExpr(expr)
	}
}

func parseExpr(expr sqlparser.Expr) Expr {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *sqlparser.ColName:
		return &ColumnExpr{
			Qualifier: e.Qualifier.Name.String(),
			Name:      e.Name.CompliantName(),
		}
	case *sqlparser.SQLVal:
		return &LiteralExpr{
			Value: parseSQLValue(e),
		}
	case sqlparser.ValTuple:
		elements := make([]Expr, 0, len(e))
		for _, el := range e {
			elements = append(elements, parseExpr(el))
		}
		return &ListExpr{Elements: elements}
	case *sqlparser.ValTuple:
		elements := make([]Expr, 0, len(*e))
		for _, el := range *e {
			elements = append(elements, parseExpr(el))
		}
		return &ListExpr{Elements: elements}
	case *sqlparser.FuncExpr:
		return parseFuncExpr(e)
	case *sqlparser.ParenExpr:
		return parseExpr(e.Expr)
	case *sqlparser.ComparisonExpr:
		return &BinaryExpr{
			Left:     parseExpr(e.Left),
			Operator: strings.ToUpper(e.Operator),
			Right:    parseExpr(e.Right),
		}
	default:
		return &LiteralExpr{Value: sqlparser.String(expr)}
	}
}

func parseExprValue(expr sqlparser.Expr) any {
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		return parseSQLValue(e)
	case *sqlparser.ColName:
		return e.Name.CompliantName()
	default:
		return sqlparser.String(expr)
	}
}

func parseSQLValue(val *sqlparser.SQLVal) any {
	if val == nil {
		return nil
	}
	switch val.Type {
	case sqlparser.IntVal:
		if num, err := strconv.Atoi(string(val.Val)); err == nil {
			return num
		}
	case sqlparser.FloatVal:
		if f, err := strconv.ParseFloat(string(val.Val), 64); err == nil {
			return f
		}
	}
	return string(val.Val)
}

func parseGroupBy(plan *Plan, groupBy sqlparser.GroupBy) {
	for _, expr := range groupBy {
		if col, ok := expr.(*sqlparser.ColName); ok {
			plan.GroupBy = append(plan.GroupBy, col.Name.CompliantName())
		}
	}
}

func parseOrderBy(plan *Plan, orderBy sqlparser.OrderBy) {
	for _, order := range orderBy {
		if col, ok := order.Expr.(*sqlparser.ColName); ok {
			dir := Asc
			if order.Direction == sqlparser.DescScr {
				dir = Desc
			}
			plan.OrderBy = append(plan.OrderBy, OrderItem{
				Field:     col.Name.CompliantName(),
				Direction: dir,
			})
		}
	}
}

func parseLimit(plan *Plan, limit *sqlparser.Limit) {
	if limit == nil {
		return
	}

	if limit.Rowcount != nil {
		if val, ok := limit.Rowcount.(*sqlparser.SQLVal); ok {
			if lim, err := strconv.ParseInt(string(val.Val), 10, 64); err == nil {
				plan.Limit = &lim
			}
		}
	}

	if limit.Offset != nil {
		if val, ok := limit.Offset.(*sqlparser.SQLVal); ok {
			if off, err := strconv.ParseInt(string(val.Val), 10, 64); err == nil {
				plan.Offset = &off
			}
		}
	}
}
