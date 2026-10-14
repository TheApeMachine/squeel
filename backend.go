package squeel

import (
	"context"

	"github.com/theapemachine/squeel/backend"
)

// Type aliases for backend adapter interfaces and implementations.
type (
	Backend[T any]  = backend.Backend[T]
	AnyBackend      = backend.AnyBackend
	Connector       = backend.Connector
	Executor[R any] = backend.Executor[R]
	AnyExecutor     = backend.AnyExecutor
	Streamer        = backend.Streamer
	QueryBuilder    = backend.QueryBuilder
	MongoBackend    = backend.MongoBackend
	MongoConfig     = backend.MongoConfig
	MongoOption     = backend.MongoOption
	MongoResult     = backend.MongoResult
	S3Backend       = backend.S3Backend
	S3Query         = backend.S3Query
	S3Config        = backend.S3Config
	S3Option        = backend.S3Option
	S3Object        = backend.S3Object
	S3ClientAPI     = backend.S3ClientAPI
	RadixS3Client   = backend.RadixS3Client
	FilterPredicate = backend.FilterPredicate
	PartitionStyle  = backend.PartitionStyle
)

const (
	HivePartitioning = backend.HivePartitioning
	FlatPartitioning = backend.FlatPartitioning
)

var (
	NewMongoBackend            = backend.NewMongoBackend
	WithMongoURI               = backend.WithMongoURI
	WithMongoDatabase          = backend.WithMongoDatabase
	WithMongoTimeout           = backend.WithMongoTimeout
	WithMongoClient            = backend.WithMongoClient
	WithMongoDB                = backend.WithMongoDB
	NewS3Backend               = backend.NewS3Backend
	NewRadixS3Client           = backend.NewRadixS3Client
	WithBucket                 = backend.WithBucket
	WithBasePrefix             = backend.WithBasePrefix
	WithPartitionStyle         = backend.WithPartitionStyle
	WithPartitionKeys          = backend.WithPartitionKeys
	WithDefaultPartitionKeys   = backend.WithDefaultPartitionKeys
	WithAutoDetectPartitions   = backend.WithAutoDetectPartitions
	WithMaxDateRangePartitions = backend.WithMaxDateRangePartitions
	WithS3Client               = backend.WithS3Client
	WithEndpoint               = backend.WithEndpoint
	WithRegion                 = backend.WithRegion
	WithCredentials            = backend.WithCredentials
	WithAnonymous              = backend.WithAnonymous
	WithUsePathStyle           = backend.WithUsePathStyle
	WithHTTPTimeout            = backend.WithHTTPTimeout
	WithDownloader             = backend.WithDownloader
	WithUploader               = backend.WithUploader
)

// Compile compiles a Statement using the provided typed backend adapter.
func Compile[T any](stmt *Statement, b Backend[T]) (T, error) {
	plan, err := stmt.Parse()
	if err != nil {
		var zero T
		return zero, err
	}
	return b.Compile(plan)
}

// Execute executes a Statement using the provided typed executor adapter.
func Execute[R any](ctx context.Context, stmt *Statement, e Executor[R]) (R, error) {
	plan, err := stmt.Parse()
	if err != nil {
		var zero R
		return zero, err
	}
	return e.Execute(ctx, plan)
}
