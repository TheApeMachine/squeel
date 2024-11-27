package squeel

import (
	errnie "github.com/theapemachine/errnie/v3"
	"github.com/xwb1989/sqlparser"
)

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
