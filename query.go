package squeel

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type Query struct {
	Context    context.Context
	Comment    string
	Operation  string
	Collection string
	Filter     bson.D
	Projection bson.D
	Sort       bson.D
	Limit      *int64
	Offset     *int64
	Pipeline   mongo.Pipeline
	Payload    bson.D
}

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

func (query *Query) VerifyContext() bool {
	return query.Context == nil
}

func (query *Query) VerifyOperation() bool {
	return query.Operation == ""
}

func (query *Query) VerifyCollection() bool {
	return query.Collection == ""
}
