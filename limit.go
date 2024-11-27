package squeel

import (
	errnie "github.com/theapemachine/errnie/v3"
	"github.com/xwb1989/sqlparser"
)

/*
parseLimit processes the LIMIT and OFFSET clauses from a SQL query and applies them
to the MongoDB query configuration. It handles both the row count (LIMIT) and offset
values, converting them from SQL value nodes to int64 pointers.

The function has special handling for LIMIT 1 queries, automatically converting them
to use MongoDB's more efficient findOne operation. If any conversion errors occur,
they are logged using the errnie error handling system.

Parameters:
- q: The Query object to modify
- node: The SQL LIMIT node containing both limit and offset information

Returns:
- The modified Query object with limit and offset settings applied
*/
func (statement *Statement) parseLimit(q *Query, node *sqlparser.Limit) *Query {
	if node == nil {
		return q
	}

	var err error

	switch value := node.Rowcount.(type) {
	case *sqlparser.SQLVal:
		if q.Limit, err = statement.makeint64Pointer(value); err != nil {
			errnie.Error(err)
			return q
		}
	}

	switch value := node.Offset.(type) {
	case *sqlparser.SQLVal:
		if q.Offset, err = statement.makeint64Pointer(value); err != nil {
			errnie.Error(err)
			return q
		}
	}

	if *q.Limit == 1 {
		q.Operation = "findone"
	}

	return q
}
