package backend

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/theapemachine/errnie"
)

// S3ClientAPI specifies the AWS S3 client operations used for object listing and downloading.
type S3ClientAPI interface {
	s3.ListObjectsV2APIClient
	manager.DownloadAPIClient
}

// PartitionStyle defines how partition keys are formatted into S3 path segments.
type PartitionStyle int

const (
	// HivePartitioning formats partitions as key=value/, e.g. "department=Engineering/".
	// This is the standard in AWS Athena, Glue, Spark, Presto, DuckDB.
	HivePartitioning PartitionStyle = iota

	// FlatPartitioning formats partitions directly as value/, e.g. "Engineering/".
	FlatPartitioning
)

// S3Config holds configuration options for the S3 backend adapter and live S3 connections.
type S3Config struct {
	Bucket                 string              // Target S3 bucket name (e.g. "my-lake-bucket")
	BasePrefix             string              // Base path prefix prepended to all table paths (e.g. "data/" or "")
	PartitionStyle         PartitionStyle      // HivePartitioning (default) or FlatPartitioning
	PartitionKeys          map[string][]string // Per-table ordered partition key hierarchy (e.g. {"orders": {"year", "month"}})
	DefaultPartitionKeys   []string            // Default partition keys for tables not in PartitionKeys
	AutoDetectPartitions   bool                // If true, detects equality/IN conditions as partition keys when none configured
	MaxDateRangePartitions int                 // Maximum days to enumerate for date range partition pruning (default 365)
	PrefetchConcurrency    int                 // Concurrent prefetch downloads in Stream pipeline (default 16)

	// Live Connection Configuration
	Endpoint        string              // Optional custom S3 endpoint (e.g. MinIO, LocalStack)
	Region          string              // AWS Region (e.g. "us-east-1", "eu-central-1")
	AccessKeyID     string              // AWS Access Key ID
	SecretAccessKey string              // AWS Secret Access Key
	SessionToken    string              // Optional AWS Session Token
	Anonymous       bool                // Connect with anonymous credentials
	UsePathStyle    bool                // Enable S3 path-style addressing
	HTTPTimeout     time.Duration       // Response header timeout for S3 requests
	Client          S3ClientAPI         // Pre-configured S3 client (e.g. *s3.Client)
	Downloader      *manager.Downloader // S3 download manager
	Uploader        *manager.Uploader   // S3 upload manager
}

// S3Option is a functional configuration option for S3Backend.
type S3Option func(*S3Config)

// WithBucket sets the S3 bucket name.
func WithBucket(bucket string) S3Option {
	return func(c *S3Config) {
		c.Bucket = bucket
	}
}

// WithBasePrefix sets a common base prefix for all table paths in S3.
func WithBasePrefix(prefix string) S3Option {
	return func(c *S3Config) {
		c.BasePrefix = prefix
	}
}

// WithPartitionStyle sets the partition path format (HivePartitioning or FlatPartitioning).
func WithPartitionStyle(style PartitionStyle) S3Option {
	return func(c *S3Config) {
		c.PartitionStyle = style
	}
}

// WithPartitionKeys specifies the ordered partition column hierarchy for a given table.
func WithPartitionKeys(table string, keys ...string) S3Option {
	return func(c *S3Config) {
		if c.PartitionKeys == nil {
			c.PartitionKeys = make(map[string][]string)
		}
		c.PartitionKeys[table] = keys
	}
}

// WithDefaultPartitionKeys sets default partition columns for any table without explicit configuration.
func WithDefaultPartitionKeys(keys ...string) S3Option {
	return func(c *S3Config) {
		c.DefaultPartitionKeys = keys
	}
}

// WithAutoDetectPartitions enables or disables partition key auto-detection.
func WithAutoDetectPartitions(auto bool) S3Option {
	return func(c *S3Config) {
		c.AutoDetectPartitions = auto
	}
}

// WithMaxDateRangePartitions sets the maximum number of daily partition prefixes to expand for date ranges.
func WithMaxDateRangePartitions(max int) S3Option {
	return func(c *S3Config) {
		c.MaxDateRangePartitions = max
	}
}

// WithPrefetchConcurrency sets the concurrent prefetch download pipeline capacity in Stream.
func WithPrefetchConcurrency(concurrency int) S3Option {
	return func(c *S3Config) {
		if concurrency > 0 {
			c.PrefetchConcurrency = concurrency
		}
	}
}

// WithS3Client sets a pre-configured S3ClientAPI (e.g. *s3.Client).
func WithS3Client(client S3ClientAPI) S3Option {
	return func(c *S3Config) {
		c.Client = client
	}
}

// WithEndpoint sets a custom S3 base endpoint (e.g. for MinIO, LocalStack, Ceph).
func WithEndpoint(endpoint string) S3Option {
	return func(c *S3Config) {
		c.Endpoint = endpoint
		c.UsePathStyle = true
	}
}

// WithRegion sets the AWS Region.
func WithRegion(region string) S3Option {
	return func(c *S3Config) {
		c.Region = region
	}
}

// WithCredentials sets static AWS credentials.
func WithCredentials(accessKeyID, secretAccessKey, sessionToken string) S3Option {
	return func(c *S3Config) {
		c.AccessKeyID = accessKeyID
		c.SecretAccessKey = secretAccessKey
		c.SessionToken = sessionToken
	}
}

// WithAnonymous sets anonymous credentials for public S3 buckets.
func WithAnonymous(anonymous bool) S3Option {
	return func(c *S3Config) {
		c.Anonymous = anonymous
	}
}

// WithUsePathStyle enables or disables path-style S3 URLs (bucket in path rather than subdomain).
func WithUsePathStyle(usePathStyle bool) S3Option {
	return func(c *S3Config) {
		c.UsePathStyle = usePathStyle
	}
}

// WithHTTPTimeout sets a custom response header timeout for HTTP requests.
func WithHTTPTimeout(timeout time.Duration) S3Option {
	return func(c *S3Config) {
		c.HTTPTimeout = timeout
	}
}

// WithDownloader sets a pre-configured S3 download manager.
func WithDownloader(downloader *manager.Downloader) S3Option {
	return func(c *S3Config) {
		c.Downloader = downloader
	}
}

// WithUploader sets a pre-configured S3 upload manager.
func WithUploader(uploader *manager.Uploader) S3Option {
	return func(c *S3Config) {
		c.Uploader = uploader
	}
}

// FilterPredicate represents a residual filter to apply in-memory or post-download.
type FilterPredicate struct {
	Column   string
	Operator string
	Value    any
}

func (p FilterPredicate) String() string {
	return fmt.Sprintf("%s %s %v", p.Column, p.Operator, p.Value)
}

// S3Query represents the compiled plan for querying data stored in S3.
// It specifies the exact S3 key prefixes that need to be downloaded, along with
// projected columns and any residual filters that must be evaluated on the records.
type S3Query struct {
	Bucket              string              // S3 bucket name
	Table               string              // Target table name
	TableBase           string              // Base path for table in S3 (e.g. "symm/" or "data/symm/")
	Prefixes            []string            // S3 key prefixes that must be listed and downloaded
	Columns             []string            // Projected column names (empty if SELECT *)
	IsStar              bool                // True if SELECT *
	Predicates          []FilterPredicate   // Residual filters to apply to downloaded data
	Limit               *int64              // Limit, if specified
	Offset              *int64              // Offset, if specified
	Distinct            bool                // Whether SELECT DISTINCT
	PartitionKeys       []string            // Partition keys recognized for this table
	PartitionStyle      PartitionStyle      // HivePartitioning or FlatPartitioning
	KeyConstraints      map[string][]string // Equality/IN constraint values for any partition key in WHERE
	OrderBy             []OrderItem         // OrderBy items from query
	PrefetchConcurrency int                 // Sliding-window prefetch download concurrency (default 16)
}

// S3URIs returns full s3:// URI strings for each generated prefix.
func (q *S3Query) S3URIs() []string {
	uris := make([]string, len(q.Prefixes))
	for i, prefix := range q.Prefixes {
		if q.Bucket != "" {
			uris[i] = fmt.Sprintf("s3://%s/%s", q.Bucket, prefix)
		} else {
			uris[i] = fmt.Sprintf("s3://%s", prefix)
		}
	}
	return uris
}

// HasResidualFilters returns true if there are filter conditions that could not be fully resolved by prefix pruning.
func (q *S3Query) HasResidualFilters() bool {
	return len(q.Predicates) > 0
}

func (q *S3Query) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("S3Query(Table: %s", q.Table))
	if q.Bucket != "" {
		sb.WriteString(fmt.Sprintf(", Bucket: %s", q.Bucket))
	}
	sb.WriteString(fmt.Sprintf(", Prefixes: %v", q.Prefixes))
	if len(q.Columns) > 0 {
		sb.WriteString(fmt.Sprintf(", Columns: %v", q.Columns))
	} else if q.IsStar {
		sb.WriteString(", Columns: [*]")
	}
	if len(q.Predicates) > 0 {
		sb.WriteString(fmt.Sprintf(", ResidualPredicates: %v", q.Predicates))
	}
	if q.Limit != nil {
		sb.WriteString(fmt.Sprintf(", Limit: %d", *q.Limit))
	}
	if q.Offset != nil {
		sb.WriteString(fmt.Sprintf(", Offset: %d", *q.Offset))
	}
	sb.WriteString(")")
	return sb.String()
}

// S3Object represents a downloaded S3 object with its key, bucket, and binary payload.
type S3Object struct {
	Key          string
	Bucket       string
	Data         []byte
	Size         int64
	LastModified *time.Time
}

// extractSequenceID attempts to extract a trailing numeric sequence identifier from the base filename of an S3 key.
// Handles formats like "1.bin", "10.bin", "part_0.bin", "part_123.bin", "p1.json", "e10.json".
func extractSequenceID(key string) (int64, bool) {
	base := path.Base(key)
	ext := path.Ext(base)
	nameWithoutExt := strings.TrimSuffix(base, ext)

	var end = -1
	var start = -1
	for i := len(nameWithoutExt) - 1; i >= 0; i-- {
		r := rune(nameWithoutExt[i])
		if unicode.IsDigit(r) {
			if end == -1 {
				end = i + 1
			}
			start = i
		} else if end != -1 {
			break
		}
	}
	if start != -1 && end != -1 {
		if val, err := strconv.ParseInt(nameWithoutExt[start:end], 10, 64); err == nil {
			return val, true
		}
	}
	return 0, false
}

// matchesKeyConstraints verifies that a key matches partition constraints, especially for
// hierarchy levels that came after a missing middle partition key in the WHERE clause.
func (q *S3Query) matchesKeyConstraints(key string) bool {
	if len(q.KeyConstraints) == 0 {
		return true
	}

	relPath := key
	if q.TableBase != "" && strings.HasPrefix(key, q.TableBase) {
		relPath = strings.TrimPrefix(key, q.TableBase)
	} else if strings.HasPrefix(key, q.Table+"/") {
		relPath = strings.TrimPrefix(key, q.Table+"/")
	}

	parts := strings.Split(relPath, "/")
	if len(parts) <= 1 {
		return true
	}

	// Partition segments before the filename
	segments := parts[:len(parts)-1]

	if q.PartitionStyle == FlatPartitioning {
		for idx, pk := range q.PartitionKeys {
			allowedVals, hasConstraint := q.KeyConstraints[pk]
			if !hasConstraint || len(allowedVals) == 0 {
				continue
			}
			if idx >= len(segments) {
				return false
			}
			segVal := segments[idx]
			matched := false
			for _, allowed := range allowedVals {
				if segVal == allowed {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
		return true
	}

	// HivePartitioning: segments are key=value
	segmentMap := make(map[string]string, len(segments))
	for _, seg := range segments {
		if k, v, found := strings.Cut(seg, "="); found {
			segmentMap[k] = v
		}
	}

	for pk, allowedVals := range q.KeyConstraints {
		if len(allowedVals) == 0 {
			continue
		}
		segVal, ok := segmentMap[pk]
		if !ok {
			return false
		}
		matched := false
		for _, allowed := range allowedVals {
			if segVal == allowed {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	return true
}

// sortObjects orders S3 objects by sequence number ascending (or descending if specified by ORDER BY),
// using the full key as deterministic tie-breaker.
func (q *S3Query) sortObjects(objects []types.Object) {
	isDesc := false
	if len(q.OrderBy) > 0 && q.OrderBy[0].Direction == Desc {
		isDesc = true
	}

	sort.Slice(objects, func(i, j int) bool {
		keyI := aws.ToString(objects[i].Key)
		keyJ := aws.ToString(objects[j].Key)

		seqI, hasSeqI := extractSequenceID(keyI)
		seqJ, hasSeqJ := extractSequenceID(keyJ)

		if hasSeqI && hasSeqJ {
			if seqI != seqJ {
				if isDesc {
					return seqI > seqJ
				}
				return seqI < seqJ
			}
			if isDesc {
				return keyI > keyJ
			}
			return keyI < keyJ
		}

		if hasSeqI != hasSeqJ {
			return hasSeqI
		}

		if isDesc {
			return keyI > keyJ
		}
		return keyI < keyJ
	})
}

// Stream streams matching objects from S3 for this compiled query in exact sequence order across prefixes,
// filtering out non-matching middle partition keys, applying OFFSET/LIMIT to metadata before downloading,
// and using a bounded sliding-window concurrent prefetch pipeline.
func (q *S3Query) Stream(
	ctx context.Context,
	client S3ClientAPI,
	downloader *manager.Downloader,
) iter.Seq2[*S3Object, error] {
	return func(yield func(*S3Object, error) bool) {
		if client == nil || downloader == nil {
			yield(nil, errnie.Error(fmt.Errorf("s3 client and downloader are required")))
			return
		}

		// 1. List lightweight metadata headers across all prefixes
		seen := make(map[string]bool)
		var matchedObjects []types.Object

		for _, prefix := range q.Prefixes {
			paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
				Bucket: aws.String(q.Bucket),
				Prefix: aws.String(prefix),
			})

			for paginator.HasMorePages() {
				page, err := paginator.NextPage(ctx)
				if err != nil {
					yield(nil, errnie.Error(err))
					return
				}

				for _, item := range page.Contents {
					key := aws.ToString(item.Key)
					if seen[key] {
						continue
					}
					seen[key] = true

					// 2. Middle partition key filtering against KeyConstraints
					if !q.matchesKeyConstraints(key) {
						continue
					}

					matchedObjects = append(matchedObjects, item)
				}
			}
		}

		// 3. Sort matched objects by sequence ID ascending (or descending if ORDER BY ... DESC)
		q.sortObjects(matchedObjects)

		// 4. Apply OFFSET and LIMIT directly to sorted slice BEFORE downloading bodies
		if q.Offset != nil && *q.Offset > 0 {
			offset := *q.Offset
			if offset >= int64(len(matchedObjects)) {
				matchedObjects = nil
			} else {
				matchedObjects = matchedObjects[offset:]
			}
		}

		if q.Limit != nil && *q.Limit >= 0 {
			limit := *q.Limit
			if limit < int64(len(matchedObjects)) {
				matchedObjects = matchedObjects[:limit]
			}
		}

		if len(matchedObjects) == 0 {
			return
		}

		// 5. Download payloads using bounded sliding-window concurrent prefetch pipeline
		concurrency := q.PrefetchConcurrency
		if concurrency <= 0 {
			concurrency = 16
		}
		if concurrency > len(matchedObjects) {
			concurrency = len(matchedObjects)
		}

		type downloadResult struct {
			obj *S3Object
			err error
		}
		type downloadTask struct {
			resCh chan downloadResult
		}

		childCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		pipeline := make(chan downloadTask, concurrency)

		// Feeder goroutine: launches bounded concurrent downloads in exact sequence order
		go func() {
			defer close(pipeline)
			sem := make(chan struct{}, concurrency)

			for _, meta := range matchedObjects {
				select {
				case <-childCtx.Done():
					return
				case sem <- struct{}{}:
				}

				resCh := make(chan downloadResult, 1)

				select {
				case <-childCtx.Done():
					return
				case pipeline <- downloadTask{resCh: resCh}:
				}

				go func(item types.Object) {
					defer func() { <-sem }()
					writer := manager.NewWriteAtBuffer(make([]byte, 0, 4096))
					_, err := downloader.Download(childCtx, writer, &s3.GetObjectInput{
						Bucket: aws.String(q.Bucket),
						Key:    item.Key,
					})
					if err != nil {
						resCh <- downloadResult{nil, err}
						return
					}
					resCh <- downloadResult{
						obj: &S3Object{
							Key:          aws.ToString(item.Key),
							Bucket:       q.Bucket,
							Data:         writer.Bytes(),
							Size:         aws.ToInt64(item.Size),
							LastModified: item.LastModified,
						},
						err: nil,
					}
				}(meta)
			}
		}()

		// Consumer loop: yields objects in exact sequence order
		for task := range pipeline {
			select {
			case <-childCtx.Done():
				yield(nil, childCtx.Err())
				return
			case res := <-task.resCh:
				if res.err != nil {
					yield(nil, errnie.Error(res.err))
					return
				}
				if !yield(res.obj, nil) {
					return
				}
			}
		}
	}
}

// GetBytes streams raw byte buffers for all matching objects in this query.
func (q *S3Query) GetBytes(
	ctx context.Context,
	client S3ClientAPI,
	downloader *manager.Downloader,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for obj, err := range q.Stream(ctx, client, downloader) {
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(obj.Data, nil) {
				return
			}
		}
	}
}

// S3Backend is an adapter that translates an abstract Plan into an S3Query with prefix pruning
// and provides streaming execution against AWS S3 compatible object stores.
type S3Backend struct {
	config S3Config
}

// S3 is an alias for S3Backend.
type S3 = S3Backend

// NewS3 creates a new S3Backend adapter configured with the provided options.
func NewS3(opts ...S3Option) *S3Backend {
	cfg := S3Config{
		PartitionStyle:         HivePartitioning,
		PartitionKeys:          make(map[string][]string),
		MaxDateRangePartitions: 365,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &S3Backend{config: cfg}
}

// NewS3Backend creates a new S3Backend adapter configured with the provided options.
func NewS3Backend(opts ...S3Option) *S3Backend {
	return NewS3(opts...)
}

// Name returns the backend identifier.
func (b *S3Backend) Name() string {
	return "s3"
}

// Connect initializes the AWS S3 client, downloader, and uploader if not already configured.
func (b *S3Backend) Connect(ctx context.Context) error {
	if b.config.Client != nil {
		if b.config.Downloader == nil {
			b.config.Downloader = manager.NewDownloader(b.config.Client)
		}
		if b.config.Uploader == nil {
			if uploaderClient, ok := b.config.Client.(manager.UploadAPIClient); ok {
				b.config.Uploader = manager.NewUploader(uploaderClient)
			}
		}
		return nil
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return errnie.Error(err)
	}

	if b.config.Region != "" {
		cfg.Region = b.config.Region
	}

	var optFns []func(*s3.Options)

	if b.config.Endpoint != "" {
		optFns = append(optFns, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(b.config.Endpoint)
			o.UsePathStyle = true
		})
	} else if b.config.UsePathStyle {
		optFns = append(optFns, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	if b.config.Anonymous {
		optFns = append(optFns, func(o *s3.Options) {
			o.Credentials = credentials.NewStaticCredentialsProvider("anonymous", "anonymous", "")
		})
	} else if b.config.AccessKeyID != "" && b.config.SecretAccessKey != "" {
		optFns = append(optFns, func(o *s3.Options) {
			o.Credentials = credentials.NewStaticCredentialsProvider(
				b.config.AccessKeyID,
				b.config.SecretAccessKey,
				b.config.SessionToken,
			)
		})
	}

	if b.config.HTTPTimeout > 0 {
		optFns = append(optFns, func(o *s3.Options) {
			o.HTTPClient = awshttp.NewBuildableClient().WithTransportOptions(func(transport *http.Transport) {
				transport.ResponseHeaderTimeout = b.config.HTTPTimeout
			})
		})
	}

	client := s3.NewFromConfig(cfg, optFns...)
	b.config.Client = client
	b.config.Downloader = manager.NewDownloader(client)
	b.config.Uploader = manager.NewUploader(client)
	return nil
}

// Client returns the underlying S3ClientAPI instance.
func (b *S3Backend) Client() S3ClientAPI {
	return b.config.Client
}

// Downloader returns the underlying manager.Downloader instance.
func (b *S3Backend) Downloader() *manager.Downloader {
	return b.config.Downloader
}

// Uploader returns the underlying manager.Uploader instance.
func (b *S3Backend) Uploader() *manager.Uploader {
	return b.config.Uploader
}

// StreamObjects compiles the plan and streams matching S3Object items using push iterators.
func (b *S3Backend) StreamObjects(ctx context.Context, plan *Plan) iter.Seq2[*S3Object, error] {
	return func(yield func(*S3Object, error) bool) {
		if err := b.Connect(ctx); err != nil {
			yield(nil, err)
			return
		}
		q, err := b.Compile(plan)
		if err != nil {
			yield(nil, err)
			return
		}
		for obj, err := range q.Stream(ctx, b.config.Client, b.config.Downloader) {
			if !yield(obj, err) {
				return
			}
		}
	}
}

// Stream compiles the plan and streams raw object byte slices, satisfying the Streamer interface.
func (b *S3Backend) Stream(ctx context.Context, plan *Plan) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for obj, err := range b.StreamObjects(ctx, plan) {
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(obj.Data, nil) {
				return
			}
		}
	}
}

// GetBytes is an alias for Stream, streaming raw object byte slices.
func (b *S3Backend) GetBytes(ctx context.Context, plan *Plan) iter.Seq2[[]byte, error] {
	return b.Stream(ctx, plan)
}

// Execute downloads and collects all matching objects for the plan, satisfying Executor[[]*S3Object].
func (b *S3Backend) Execute(ctx context.Context, plan *Plan) ([]*S3Object, error) {
	var objects []*S3Object
	for obj, err := range b.StreamObjects(ctx, plan) {
		if err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}
	return objects, nil
}

// ExecuteAny implements AnyExecutor for dynamic query execution dispatch.
func (b *S3Backend) ExecuteAny(ctx context.Context, plan *Plan) (any, error) {
	return b.Execute(ctx, plan)
}

// Upload uploads raw bytes to a target key in the configured S3 bucket using manager.Uploader.
func (b *S3Backend) Upload(ctx context.Context, key string, data []byte) (*manager.UploadOutput, error) {
	if err := b.Connect(ctx); err != nil {
		return nil, err
	}
	if b.config.Uploader == nil {
		return nil, errnie.Error(fmt.Errorf("s3 uploader is not initialized"))
	}
	return b.config.Uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(b.config.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
}

// CompileAny implements AnyBackend.
func (b *S3Backend) CompileAny(plan *Plan) (any, error) {
	return b.Compile(plan)
}

// Compile translates an abstract Plan into an S3Query.
func (b *S3Backend) Compile(plan *Plan) (*S3Query, error) {
	if plan == nil {
		return nil, fmt.Errorf("plan is nil")
	}

	tableName := plan.Table.Name
	if tableName == "" && len(plan.Joins) > 0 {
		tableName = plan.Joins[0].Table.Name
	}

	// 1. Determine table base prefix
	tableBase := tableName + "/"
	if b.config.BasePrefix != "" {
		base := strings.Trim(b.config.BasePrefix, "/")
		tableBase = base + "/" + tableBase
	}

	// 2. Identify partition keys for this table
	partKeys := b.getPartitionKeys(tableName, plan.Where)

	// 3. Generate S3 prefixes via partition pruning
	prefixes, usedCols, keyConstraints := b.generatePrefixes(tableBase, partKeys, plan.Where)

	// 4. Extract projections / columns
	columns, isStar := b.extractColumns(plan)

	// 5. Extract residual predicates (filters not fully absorbed by partition prefixes)
	predicates := b.extractResidualPredicates(plan.Where, usedCols)

	concurrency := b.config.PrefetchConcurrency
	if concurrency <= 0 {
		concurrency = 16
	}

	query := &S3Query{
		Bucket:              b.config.Bucket,
		Table:               tableName,
		TableBase:           tableBase,
		Prefixes:            prefixes,
		Columns:             columns,
		IsStar:              isStar,
		Predicates:          predicates,
		Limit:               plan.Limit,
		Offset:              plan.Offset,
		Distinct:            plan.Distinct,
		PartitionKeys:       partKeys,
		PartitionStyle:      b.config.PartitionStyle,
		KeyConstraints:      keyConstraints,
		OrderBy:             plan.OrderBy,
		PrefetchConcurrency: concurrency,
	}

	return query, nil
}

func (b *S3Backend) getPartitionKeys(table string, where Expr) []string {
	if keys, ok := b.config.PartitionKeys[table]; ok && len(keys) > 0 {
		return keys
	}
	if len(b.config.DefaultPartitionKeys) > 0 {
		return b.config.DefaultPartitionKeys
	}
	if b.config.AutoDetectPartitions && where != nil {
		return b.detectPartitionCandidates(where)
	}
	return nil
}

func (b *S3Backend) detectPartitionCandidates(where Expr) []string {
	var candidates []string
	seen := make(map[string]bool)

	var walk func(e Expr)
	walk = func(e Expr) {
		if e == nil {
			return
		}
		switch node := e.(type) {
		case *BinaryExpr:
			if strings.EqualFold(node.Operator, "=") || strings.EqualFold(node.Operator, "IN") {
				if col, ok := node.Left.(*ColumnExpr); ok {
					name := col.Name
					if !seen[name] {
						seen[name] = true
						candidates = append(candidates, name)
					}
				}
			}
		case *LogicalExpr:
			for _, sub := range node.Exprs {
				walk(sub)
			}
		}
	}

	walk(where)
	return candidates
}

type partitionConstraint struct {
	values   []string
	isRange  bool
	rangeMin string
	rangeMax string
}

func (b *S3Backend) generatePrefixes(tableBase string, partKeys []string, where Expr) ([]string, map[string]bool, map[string][]string) {
	usedCols := make(map[string]bool)
	keyConstraints := make(map[string][]string)

	if len(partKeys) == 0 || where == nil {
		return []string{tableBase}, usedCols, keyConstraints
	}

	// Extract constraints for partition keys
	constraints := make(map[string]*partitionConstraint)
	b.collectConstraints(where, constraints)

	for _, pk := range partKeys {
		if c, ok := constraints[pk]; ok {
			if c.isRange {
				rangeVals := b.expandDateRange(c.rangeMin, c.rangeMax)
				if len(rangeVals) > 0 {
					keyConstraints[pk] = rangeVals
				}
			} else if len(c.values) > 0 {
				keyConstraints[pk] = c.values
			}
		}
	}

	// Check for top-level OR conditions on partition keys
	if orPrefixes, ok := b.handleTopLevelOR(tableBase, partKeys, where, usedCols); ok {
		return orPrefixes, usedCols, keyConstraints
	}

	currentPrefixes := []string{tableBase}

	for _, key := range partKeys {
		constraint, ok := constraints[key]
		if !ok {
			// Partition key hierarchy broken: cannot prune deeper
			break
		}

		usedCols[key] = true
		var nextPrefixes []string

		if constraint.isRange {
			rangeVals := b.expandDateRange(constraint.rangeMin, constraint.rangeMax)
			if len(rangeVals) > 0 {
				for _, p := range currentPrefixes {
					for _, val := range rangeVals {
						nextPrefixes = append(nextPrefixes, p+b.formatPartitionSegment(key, val))
					}
				}
				currentPrefixes = nextPrefixes
				continue
			}
			// If range couldn't be expanded (e.g. too broad), stop pruning at this level
			break
		}

		if len(constraint.values) == 0 {
			break
		}

		for _, p := range currentPrefixes {
			for _, val := range constraint.values {
				nextPrefixes = append(nextPrefixes, p+b.formatPartitionSegment(key, val))
			}
		}

		currentPrefixes = nextPrefixes
	}

	return currentPrefixes, usedCols, keyConstraints
}

func (b *S3Backend) handleTopLevelOR(tableBase string, partKeys []string, where Expr, usedCols map[string]bool) ([]string, bool) {
	logExpr, ok := where.(*LogicalExpr)
	if !ok || !strings.EqualFold(logExpr.Operator, "OR") {
		return nil, false
	}

	if len(partKeys) == 0 {
		return nil, false
	}
	primaryKey := partKeys[0]

	var allValues []string
	for _, sub := range logExpr.Exprs {
		if bin, ok := sub.(*BinaryExpr); ok && strings.EqualFold(bin.Operator, "=") {
			if col, ok := bin.Left.(*ColumnExpr); ok && col.Name == primaryKey {
				val := fmt.Sprintf("%v", extractValue(bin.Right))
				allValues = append(allValues, val)
				continue
			}
		}
		return nil, false
	}

	usedCols[primaryKey] = true
	prefixes := make([]string, len(allValues))
	for i, val := range allValues {
		prefixes[i] = tableBase + b.formatPartitionSegment(primaryKey, val)
	}
	return prefixes, true
}

func (b *S3Backend) collectConstraints(expr Expr, constraints map[string]*partitionConstraint) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *BinaryExpr:
		b.collectBinaryConstraint(e, constraints)

	case *LogicalExpr:
		if strings.EqualFold(e.Operator, "AND") {
			for _, sub := range e.Exprs {
				b.collectConstraints(sub, constraints)
			}
		} else if strings.EqualFold(e.Operator, "OR") {
			var orCol string
			var orVals []string
			isHomogeneous := true

			for _, sub := range e.Exprs {
				if bin, ok := sub.(*BinaryExpr); ok && strings.EqualFold(bin.Operator, "=") {
					if col, ok := bin.Left.(*ColumnExpr); ok {
						if orCol == "" {
							orCol = col.Name
						} else if orCol != col.Name {
							isHomogeneous = false
							break
						}
						orVals = append(orVals, fmt.Sprintf("%v", extractValue(bin.Right)))
						continue
					}
				}
				isHomogeneous = false
				break
			}

			if isHomogeneous && orCol != "" {
				c := getOrCreateConstraint(constraints, orCol)
				c.values = append(c.values, orVals...)
			}
		}

	case *RangeExpr:
		c := getOrCreateConstraint(constraints, e.Field)
		c.isRange = true
		c.rangeMin = fmt.Sprintf("%v", e.From)
		c.rangeMax = fmt.Sprintf("%v", e.To)
	}
}

func (b *S3Backend) collectBinaryConstraint(e *BinaryExpr, constraints map[string]*partitionConstraint) {
	col, ok := e.Left.(*ColumnExpr)
	if !ok {
		return
	}
	colName := col.Name

	op := strings.ToUpper(e.Operator)
	switch op {
	case "=":
		val := fmt.Sprintf("%v", extractValue(e.Right))
		c := getOrCreateConstraint(constraints, colName)
		c.values = append(c.values, val)

	case "IN":
		c := getOrCreateConstraint(constraints, colName)
		for _, v := range extractSlice(e.Right) {
			c.values = append(c.values, fmt.Sprintf("%v", v))
		}

	case ">=":
		c := getOrCreateConstraint(constraints, colName)
		c.isRange = true
		c.rangeMin = fmt.Sprintf("%v", extractValue(e.Right))

	case "<=":
		c := getOrCreateConstraint(constraints, colName)
		c.isRange = true
		c.rangeMax = fmt.Sprintf("%v", extractValue(e.Right))
	}
}

func getOrCreateConstraint(constraints map[string]*partitionConstraint, col string) *partitionConstraint {
	if c, ok := constraints[col]; ok {
		return c
	}
	c := &partitionConstraint{}
	constraints[col] = c
	return c
}

func (b *S3Backend) formatPartitionSegment(key, val string) string {
	if b.config.PartitionStyle == FlatPartitioning {
		return val + "/"
	}
	return fmt.Sprintf("%s=%s/", key, val)
}

func (b *S3Backend) expandDateRange(startStr, endStr string) []string {
	if startStr == "" || endStr == "" {
		return nil
	}

	formats := []string{"2006-01-02", "2006/01/02"}
	var start, end time.Time
	var err error

	for _, fmtStr := range formats {
		start, err = time.Parse(fmtStr, startStr)
		if err == nil {
			end, err = time.Parse(fmtStr, endStr)
			if err == nil {
				break
			}
		}
	}

	if err != nil || start.After(end) {
		return nil
	}

	maxDays := b.config.MaxDateRangePartitions
	if maxDays <= 0 {
		maxDays = 365
	}

	days := int(end.Sub(start).Hours()/24) + 1
	if days > maxDays {
		return nil
	}

	var dates []string
	for cur := start; !cur.After(end); cur = cur.AddDate(0, 0, 1) {
		dates = append(dates, cur.Format("2006-01-02"))
	}
	return dates
}

func (b *S3Backend) extractColumns(plan *Plan) ([]string, bool) {
	var cols []string
	isStar := false

	for _, p := range plan.Projections {
		if p.IsStar {
			isStar = true
			continue
		}
		if p.Alias != "" {
			cols = append(cols, p.Alias)
		} else if col, ok := p.Expr.(*ColumnExpr); ok {
			cols = append(cols, col.Name)
		} else if fn, ok := p.Expr.(*FuncExpr); ok {
			cols = append(cols, fn.String())
		} else if p.Expr != nil {
			cols = append(cols, p.Expr.String())
		}
	}

	return cols, isStar
}

func (b *S3Backend) extractResidualPredicates(where Expr, usedCols map[string]bool) []FilterPredicate {
	var predicates []FilterPredicate
	if where == nil {
		return predicates
	}

	var walk func(e Expr)
	walk = func(e Expr) {
		if e == nil {
			return
		}
		switch node := e.(type) {
		case *BinaryExpr:
			if col, ok := node.Left.(*ColumnExpr); ok {
				op := strings.ToUpper(node.Operator)
				if usedCols[col.Name] && (op == "=" || op == "IN") {
					return
				}
				predicates = append(predicates, FilterPredicate{
					Column:   col.FullName(),
					Operator: node.Operator,
					Value:    extractValue(node.Right),
				})
			}
		case *RangeExpr:
			if !usedCols[node.Field] {
				predicates = append(predicates, FilterPredicate{
					Column:   node.Field,
					Operator: "BETWEEN",
					Value:    []any{node.From, node.To},
				})
			}
		case *FuncExpr:
			predicates = append(predicates, FilterPredicate{
				Column:   node.Name,
				Operator: "CALL",
				Value:    node.String(),
			})
		case *LogicalExpr:
			for _, sub := range node.Exprs {
				walk(sub)
			}
		}
	}

	walk(where)
	return predicates
}
