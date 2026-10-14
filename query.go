package squeel

import (
	"github.com/theapemachine/squeel/backend"
	"go.mongodb.org/mongo-driver/bson"
)

// Query is an alias to backend.Query.
type Query = backend.Query

// NewQuery creates a new Query instance.
func NewQuery() *Query {
	return backend.NewQuery()
}

// Map converts a bson.D to a bson.M.
func Map(d bson.D) bson.M {
	return backend.Map(d)
}
