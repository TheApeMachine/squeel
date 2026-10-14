package backend

import (
	"context"
	"iter"
)

// Backend defines the adapter contract that database and storage backends implement
// to compile an abstract Plan into their specific query or execution plan.
type Backend[T any] interface {
	// Name returns the identifier of the backend (e.g. "mongodb", "s3").
	Name() string

	// Compile transforms the abstract query plan into the backend's target representation.
	Compile(plan *Plan) (T, error)
}

// AnyBackend provides a type-erased backend interface for dynamic backend dispatch.
type AnyBackend interface {
	Name() string
	CompileAny(plan *Plan) (any, error)
}

// Connector defines the lifecycle interface for backends that maintain connections to external systems.
type Connector interface {
	Connect(ctx context.Context) error
	Close(ctx context.Context) error
}

// Executor defines the generic execution contract for backends that run queries and return typed results.
type Executor[R any] interface {
	Execute(ctx context.Context, plan *Plan) (R, error)
}

// AnyExecutor provides a type-erased execution interface for dynamic query dispatch.
type AnyExecutor interface {
	ExecuteAny(ctx context.Context, plan *Plan) (any, error)
}

// Streamer defines the streaming interface for backends that yield data streams.
type Streamer interface {
	Stream(ctx context.Context, plan *Plan) iter.Seq2[[]byte, error]
}

// QueryBuilder defines an optional interface for backends that support mutating or populating an existing target structure.
type QueryBuilder interface {
	Build(plan *Plan, target any) (any, error)
}

// Compile compiles a Plan using the provided backend adapter.
func Compile[T any](plan *Plan, backend Backend[T]) (T, error) {
	return backend.Compile(plan)
}

// Execute executes a Plan using the provided executor adapter.
func Execute[R any](ctx context.Context, plan *Plan, executor Executor[R]) (R, error) {
	return executor.Execute(ctx, plan)
}
