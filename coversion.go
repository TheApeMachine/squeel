package squeel

import "github.com/xwb1989/sqlparser"

func (statement *Statement) aliasedTableName(expr interface{}) string {
	return expr.(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.CompliantName()
}

func (statement *Statement) on(node *sqlparser.JoinTableExpr) *sqlparser.ComparisonExpr {
	return node.Condition.On.(*sqlparser.ComparisonExpr)
}

func (statement *Statement) colName(col interface{}) string {
	return col.(*sqlparser.ColName).Name.CompliantName()
}
