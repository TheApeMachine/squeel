package backend

import (
	"fmt"
	"strings"
)

/*
Plan is an abstract, storage-agnostic intermediate representation of a parsed SQL query.
It captures the intent of the SQL query without tying it to any specific database or
storage engine (e.g. MongoDB, S3, SQL databases).
*/
type Plan struct {
	Raw         string       // The original raw SQL string
	Table       TableRef     // Primary table or collection
	Joins       []Join       // Any table joins
	Projections []Projection // Selected expressions, columns, or aggregates
	Distinct    bool         // Whether the query specifies DISTINCT
	Where       Expr         // Filter expression tree
	GroupBy     []string     // Grouping column names
	Having      Expr         // Post-aggregation having filter expression tree
	OrderBy     []OrderItem  // Sort ordering specifications
	Limit       *int64       // Row limit, if specified
	Offset      *int64       // Row offset / skip, if specified
}

// NewPlan constructs an empty Plan.
func NewPlan(raw string) *Plan {
	return &Plan{
		Raw:         raw,
		Joins:       make([]Join, 0),
		Projections: make([]Projection, 0),
		GroupBy:     make([]string, 0),
		OrderBy:     make([]OrderItem, 0),
	}
}

// TableRef represents a referenced table or collection, including any alias.
type TableRef struct {
	Name  string // The table or collection name
	Alias string // Optional alias (e.g. "users u" -> Alias: "u")
}

func (t TableRef) String() string {
	if t.Alias != "" && t.Alias != t.Name {
		return fmt.Sprintf("%s AS %s", t.Name, t.Alias)
	}
	return t.Name
}

// Join represents a JOIN clause between tables.
type Join struct {
	Type       string   // "JOIN", "LEFT JOIN", "INNER JOIN", "RIGHT JOIN"
	Table      TableRef // Joined table reference
	On         Expr     // The ON condition expression
	LeftField  string   // Extracted left column name in the join condition
	RightField string   // Extracted right column name in the join condition
}

// Projection represents a projected item in the SELECT list.
type Projection struct {
	Expr     Expr   // Expression being selected (ColumnExpr, FuncExpr, LiteralExpr, etc.)
	Alias    string // Optional column alias
	Distinct bool   // Whether this projection is DISTINCT
	IsStar   bool   // Whether this represents a wildcard (*)
}

// OrderDirection specifies sorting direction.
type OrderDirection int

const (
	Asc OrderDirection = iota
	Desc
)

func (d OrderDirection) String() string {
	if d == Desc {
		return "DESC"
	}
	return "ASC"
}

// OrderItem specifies an ORDER BY item with a field and direction.
type OrderItem struct {
	Field     string
	Direction OrderDirection
}

// Expr represents an abstract expression node in the plan AST.
type Expr interface {
	exprNode()
	String() string
}

// ColumnExpr represents a column reference (e.g., "id", "users.id", "theme.nl").
type ColumnExpr struct {
	Qualifier string // Table qualifier or alias, if any
	Name      string // Column name
}

func (*ColumnExpr) exprNode() {}

func (c *ColumnExpr) String() string {
	if c.Qualifier != "" {
		return c.Qualifier + "." + c.Name
	}
	return c.Name
}

// FullName returns the compliant column name, combining qualifier and name when appropriate.
func (c *ColumnExpr) FullName() string {
	if c.Qualifier != "" {
		return strings.TrimPrefix(c.Qualifier+"."+c.Name, ".")
	}
	return c.Name
}

// LiteralExpr represents a literal constant value (string, int64, float64, bool, etc.).
type LiteralExpr struct {
	Value any
}

func (*LiteralExpr) exprNode() {}

func (l *LiteralExpr) String() string {
	switch v := l.Value.(type) {
	case string:
		return fmt.Sprintf("'%s'", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// BinaryExpr represents a binary operation: Left Operator Right.
// Supported operators include: "=", "!=", ">", ">=", "<", "<=", "LIKE", "IN".
type BinaryExpr struct {
	Left     Expr
	Operator string
	Right    Expr
}

func (*BinaryExpr) exprNode() {}

func (b *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left, b.Operator, b.Right)
}

// RangeExpr represents a BETWEEN condition: Field BETWEEN From AND To.
type RangeExpr struct {
	Field string
	From  any
	To    any
}

func (*RangeExpr) exprNode() {}

func (r *RangeExpr) String() string {
	return fmt.Sprintf("(%s BETWEEN %v AND %v)", r.Field, r.From, r.To)
}

// LogicalExpr represents a logical operation: AND, OR, NOT.
type LogicalExpr struct {
	Operator string // "AND", "OR", "NOT"
	Exprs    []Expr
}

func (*LogicalExpr) exprNode() {}

func (l *LogicalExpr) String() string {
	parts := make([]string, len(l.Exprs))
	for i, e := range l.Exprs {
		parts[i] = e.String()
	}
	return fmt.Sprintf("(%s)", strings.Join(parts, " "+l.Operator+" "))
}

// FuncExpr represents a function invocation: Name(Args...).
type FuncExpr struct {
	Name     string
	Args     []Expr
	Distinct bool
}

func (*FuncExpr) exprNode() {}

func (f *FuncExpr) String() string {
	argStrs := make([]string, len(f.Args))
	for i, a := range f.Args {
		argStrs[i] = a.String()
	}
	prefix := ""
	if f.Distinct {
		prefix = "DISTINCT "
	}
	return fmt.Sprintf("%s(%s%s)", strings.ToUpper(f.Name), prefix, strings.Join(argStrs, ", "))
}

// ListExpr represents a tuple or list of expressions, e.g. in IN ('a', 'b').
type ListExpr struct {
	Elements []Expr
}

func (*ListExpr) exprNode() {}

func (l *ListExpr) String() string {
	elemStrs := make([]string, len(l.Elements))
	for i, e := range l.Elements {
		elemStrs[i] = e.String()
	}
	return fmt.Sprintf("(%s)", strings.Join(elemStrs, ", "))
}

// StarExpr represents a wildcard selector (* or table.*).
type StarExpr struct {
	Qualifier string
}

func (*StarExpr) exprNode() {}

func (s *StarExpr) String() string {
	if s.Qualifier != "" {
		return s.Qualifier + ".*"
	}
	return "*"
}

// SubqueryExpr represents a nested subquery.
type SubqueryExpr struct {
	Plan *Plan
}

func (*SubqueryExpr) exprNode() {}

func (s *SubqueryExpr) String() string {
	if s.Plan != nil {
		return fmt.Sprintf("(%s)", s.Plan.Raw)
	}
	return "(SUBQUERY)"
}
