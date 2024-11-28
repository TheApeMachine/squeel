package squeel

import "github.com/xwb1989/sqlparser"

/*
aliasedTableName extracts the compliant table name from an aliased table expression.
It expects an *sqlparser.AliasedTableExpr that contains a TableName and returns
the compliant version of that name.
*/
func (statement *Statement) aliasedTableName(expr interface{}) string {
	switch e := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tableName, ok := e.Expr.(sqlparser.TableName); ok {
			return tableName.Name.CompliantName()
		}
	case *sqlparser.JoinTableExpr:
		if aliased, ok := e.LeftExpr.(*sqlparser.AliasedTableExpr); ok {
			if tableName, ok := aliased.Expr.(sqlparser.TableName); ok {
				return tableName.Name.CompliantName()
			}
		}
	}
	return ""
}

/*
on extracts the comparison expression from a JOIN...ON clause. It takes a JoinTableExpr
node and returns the ComparisonExpr that represents the ON condition.
*/
func (statement *Statement) on(node *sqlparser.JoinTableExpr) *sqlparser.ComparisonExpr {
	return node.Condition.On.(*sqlparser.ComparisonExpr)
}

/*
colName extracts the compliant column name from a column expression. It takes
a column expression interface and returns the compliant version of the column name.
This is used for standardizing column names across different SQL dialects.
*/
func (statement *Statement) colName(col interface{}) string {
	switch e := col.(type) {
	case *sqlparser.ColName:
		return e.Name.CompliantName()
	case *sqlparser.Subquery:
		// For subqueries, we'll return an empty string as they need special handling
		return ""
	default:
		return ""
	}
}
