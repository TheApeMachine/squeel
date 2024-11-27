package squeel

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

/*
Query represents a MongoDB query configuration that can be built from SQL statements.
It contains all the necessary components to execute various types of MongoDB operations,
including find, aggregate, and distinct queries.
*/
type Query struct {
	Context    context.Context // Context for the query execution
	Comment    string          // Optional comment for query logging/debugging
	Operation  string          // Type of MongoDB operation (find, aggregate, etc.)
	Collection string          // Target MongoDB collection
	Filter     bson.D          // Query filter criteria
	Projection bson.D          // Field projection specification
	Sort       bson.D          // Sort order specification
	Limit      *int64          // Maximum number of documents to return
	Offset     *int64          // Number of documents to skip
	Pipeline   mongo.Pipeline  // Aggregation pipeline stages
	Payload    bson.D          // Additional query parameters
}

/*
NewQuery creates a new Query instance with initialized slices and default values.
It sets up an empty context and initializes all the necessary BSON document
slices that will be populated during query building.

Returns:
- A new Query instance ready for configuration
*/
func NewQuery() *Query {
	return &Query{
		Context:    context.Background(),
		Comment:    "data request",
		Filter:     make(bson.D, 0),
		Projection: make(bson.D, 0),
		Sort:       make(bson.D, 0),
		Payload:    make(bson.D, 0),
		Pipeline:   make(mongo.Pipeline, 0),
	}
}

/*
Fails performs validation checks on the Query instance to ensure it has all
required components before execution. It runs a series of verification functions
and returns true if any validation fails.

Returns:
- true if the query is invalid or incomplete
- false if the query is valid and ready for execution
*/
func (query *Query) Fails() bool {
	for _, fn := range []func() bool{
		query.VerifyContext,
		query.VerifyOperation,
		query.VerifyCollection,
	} {
		if fn() {
			return true
		}
	}

	return false
}

/*
VerifyContext checks if the query has a valid context.

Returns:
- true if the context is nil
- false if the context is valid
*/
func (query *Query) VerifyContext() bool {
	return query.Context == nil
}

/*
VerifyOperation checks if the query has a specified operation type.

Returns:
- true if the operation is empty
- false if an operation is specified
*/
func (query *Query) VerifyOperation() bool {
	return query.Operation == ""
}

/*
VerifyCollection checks if the query has a target collection specified.

Returns:
- true if the collection name is empty
- false if a collection is specified
*/
func (query *Query) VerifyCollection() bool {
	return query.Collection == ""
}
