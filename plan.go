package squeel

import "github.com/theapemachine/squeel/backend"

// Type aliases for Plan and abstract AST expression types defined in backend package.
type (
	Plan           = backend.Plan
	TableRef       = backend.TableRef
	Join           = backend.Join
	Projection     = backend.Projection
	OrderItem      = backend.OrderItem
	OrderDirection = backend.OrderDirection

	Expr         = backend.Expr
	ColumnExpr   = backend.ColumnExpr
	LiteralExpr  = backend.LiteralExpr
	BinaryExpr   = backend.BinaryExpr
	RangeExpr    = backend.RangeExpr
	LogicalExpr  = backend.LogicalExpr
	FuncExpr     = backend.FuncExpr
	ListExpr     = backend.ListExpr
	StarExpr     = backend.StarExpr
	SubqueryExpr = backend.SubqueryExpr
)

const (
	Asc  = backend.Asc
	Desc = backend.Desc
)

// NewPlan constructs an empty Plan.
func NewPlan(raw string) *Plan {
	return backend.NewPlan(raw)
}
