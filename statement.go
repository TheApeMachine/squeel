package squeel

import (
	"context"
	"fmt"
	"iter"

	errnie "github.com/theapemachine/errnie/v3"
	"github.com/theapemachine/squeel/backend"
)

func init() {
	backend.SetSQLHandler(func(sql string, query *backend.Query) *backend.Query {
		stmt := NewStatement(sql)
		var err error
		if query, err = stmt.Build(query); errnie.Error(err) != nil {
			return nil
		}
		return query
	})
}

/*
Statement represents a SQL statement that can be parsed into an abstract Plan
and compiled or executed across any backend implementing the adapter interfaces.
*/
type Statement struct {
	raw     string
	plan    *Plan
	err     error
	backend AnyBackend
}

// NewStatement creates a new Statement instance from a raw SQL query string and an optional backend interface.
func NewStatement(raw string, b ...AnyBackend) *Statement {
	stmt := &Statement{raw: raw}
	if len(b) > 0 && b[0] != nil {
		stmt.backend = b[0]
	}
	return stmt
}

// WithBackend attaches a backend adapter interface to the statement.
func (statement *Statement) WithBackend(b AnyBackend) *Statement {
	statement.backend = b
	return statement
}

// Backend returns the currently attached backend adapter interface, if any.
func (statement *Statement) Backend() AnyBackend {
	return statement.backend
}

// Parse parses the raw SQL statement into an abstract, storage-agnostic Plan.
func (statement *Statement) Parse() (*Plan, error) {
	if statement == nil {
		return nil, errnie.Error(fmt.Errorf("statement is nil"))
	}
	if statement.err != nil {
		return nil, statement.err
	}
	if statement.plan != nil {
		return statement.plan, nil
	}

	plan, err := ParseSQL(statement.raw)
	if err != nil {
		statement.err = err
		return nil, err
	}
	statement.plan = plan
	return plan, nil
}

// Compile compiles the statement using the provided backend interface, or the statement's attached backend.
func (statement *Statement) Compile(b ...AnyBackend) (any, error) {
	plan, err := statement.Parse()
	if err != nil {
		return nil, err
	}
	target := statement.backend
	if len(b) > 0 && b[0] != nil {
		target = b[0]
	}
	if target == nil {
		return nil, errnie.Error(fmt.Errorf("no backend configured for statement compilation"))
	}
	return target.CompileAny(plan)
}

// Execute executes the statement using the provided executor interface, or the statement's attached backend.
func (statement *Statement) Execute(ctx context.Context, executor ...AnyExecutor) (any, error) {
	plan, err := statement.Parse()
	if err != nil {
		return nil, err
	}
	var exec AnyExecutor
	if len(executor) > 0 && executor[0] != nil {
		exec = executor[0]
	} else if e, ok := statement.backend.(AnyExecutor); ok {
		exec = e
	}
	if exec == nil {
		return nil, errnie.Error(fmt.Errorf("backend does not implement Executor interface"))
	}
	return exec.ExecuteAny(ctx, plan)
}

// Stream streams data for the statement using the provided streamer interface, or the statement's attached backend.
func (statement *Statement) Stream(ctx context.Context, streamer ...Streamer) iter.Seq2[[]byte, error] {
	plan, err := statement.Parse()
	if err != nil {
		return func(yield func([]byte, error) bool) {
			yield(nil, err)
		}
	}
	var s Streamer
	if len(streamer) > 0 && streamer[0] != nil {
		s = streamer[0]
	} else if st, ok := statement.backend.(Streamer); ok {
		s = st
	}
	if s == nil {
		return func(yield func([]byte, error) bool) {
			yield(nil, errnie.Error(fmt.Errorf("backend does not implement Streamer interface")))
		}
	}
	return s.Stream(ctx, plan)
}

// Build compiles the SQL statement into the target query structure using the attached backend's QueryBuilder interface.
// If no backend is attached, it defaults to using the MongoDB QueryBuilder.
func (statement *Statement) Build(target any) (*Query, error) {
	plan, err := statement.Parse()
	if err != nil {
		if q, ok := target.(*Query); ok {
			return q, err
		}
		return nil, err
	}

	if qb, ok := statement.backend.(QueryBuilder); ok {
		res, err := qb.Build(plan, target)
		if q, ok := res.(*Query); ok {
			return q, err
		}
		return nil, err
	}

	// Default fallback to MongoDB query builder if target is *Query
	if q, ok := target.(*Query); ok {
		return NewMongoBackend().CompileWithQuery(plan, q)
	}
	return nil, errnie.Error(fmt.Errorf("unsupported build target type %T", target))
}
