package backend

import (
	"context"
	b64 "encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/theapemachine/errnie"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	mongoUUIDRegex = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)
)

// MongoConfig holds connection parameters and client references for MongoDB.
type MongoConfig struct {
	URI      string
	Database string
	Timeout  time.Duration
	Client   *mongo.Client
	DB       *mongo.Database
}

// MongoOption is a functional configuration option for MongoBackend.
type MongoOption func(*MongoConfig)

// WithMongoURI sets the connection URI string for MongoDB.
func WithMongoURI(uri string) MongoOption {
	return func(c *MongoConfig) {
		c.URI = uri
	}
}

// WithMongoDatabase sets the target database name.
func WithMongoDatabase(name string) MongoOption {
	return func(c *MongoConfig) {
		c.Database = name
	}
}

// WithMongoTimeout sets the connection/operation timeout.
func WithMongoTimeout(timeout time.Duration) MongoOption {
	return func(c *MongoConfig) {
		c.Timeout = timeout
	}
}

// WithMongoClient sets a pre-existing mongo.Client instance.
func WithMongoClient(client *mongo.Client) MongoOption {
	return func(c *MongoConfig) {
		c.Client = client
	}
}

// WithMongoDB sets a pre-existing mongo.Database instance.
func WithMongoDB(db *mongo.Database) MongoOption {
	return func(c *MongoConfig) {
		c.DB = db
	}
}

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

// MongoQuery is an alias for Query.
type MongoQuery = Query

// NewQuery creates a new Query instance with initialized slices and default values.
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

// NewMongoQuery creates a new MongoQuery instance.
func NewMongoQuery() *Query {
	return NewQuery()
}

// Fails performs validation checks on the Query instance.
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

// VerifyContext checks if the query has a valid context.
func (query *Query) VerifyContext() bool {
	return query.Context == nil
}

// VerifyOperation checks if the query has a specified operation type.
func (query *Query) VerifyOperation() bool {
	return query.Operation == ""
}

// VerifyCollection checks if the query has a target collection specified.
func (query *Query) VerifyCollection() bool {
	return query.Collection == ""
}

/*
MongoBackend is an adapter that translates an abstract Plan into a MongoDB Query configuration
and manages live connections to MongoDB for query execution.
*/
type MongoBackend struct {
	config MongoConfig
}

// NewMongo creates a new MongoBackend adapter instance with optional connection settings.
func NewMongo(opts ...MongoOption) *MongoBackend {
	b := &MongoBackend{}
	for _, opt := range opts {
		opt(&b.config)
	}
	return b
}

// NewMongoBackend creates a new MongoBackend adapter instance with optional connection settings.
func NewMongoBackend(opts ...MongoOption) *MongoBackend {
	return NewMongo(opts...)
}

// Name returns the backend identifier.
func (b *MongoBackend) Name() string {
	return "mongodb"
}

// Connect initializes the MongoDB client and database connection if not already connected.
func (b *MongoBackend) Connect(ctx context.Context) error {
	if b.config.DB != nil {
		return nil
	}
	if b.config.Client == nil {
		if b.config.URI == "" {
			return errnie.Error(fmt.Errorf("mongodb URI or client is required to connect"))
		}
		clientOptions := options.Client().ApplyURI(b.config.URI)
		if b.config.Timeout > 0 {
			clientOptions.SetTimeout(b.config.Timeout)
		}
		client, err := mongo.Connect(ctx, clientOptions)
		if err != nil {
			return errnie.Error(err)
		}
		b.config.Client = client
	}
	if b.config.Database == "" {
		return errnie.Error(fmt.Errorf("mongodb database name is required"))
	}
	b.config.DB = b.config.Client.Database(b.config.Database)
	return nil
}

// Close disconnects the MongoDB client if it was opened by this backend.
func (b *MongoBackend) Close(ctx context.Context) error {
	if b.config.Client != nil {
		return b.config.Client.Disconnect(ctx)
	}
	return nil
}

// Client returns the underlying *mongo.Client, or nil if not connected.
func (b *MongoBackend) Client() *mongo.Client {
	return b.config.Client
}

// Database returns the underlying *mongo.Database, or nil if not connected.
func (b *MongoBackend) Database() *mongo.Database {
	return b.config.DB
}

// MongoResult wraps the result of executing a Query against MongoDB.
type MongoResult struct {
	Operation      string
	Cursor         *mongo.Cursor
	SingleResult   *mongo.SingleResult
	Count          int64
	DistinctValues []any
}

// Close closes the underlying cursor if one was created.
func (r *MongoResult) Close(ctx context.Context) error {
	if r.Cursor != nil {
		return r.Cursor.Close(ctx)
	}
	return nil
}

// All decodes all documents into the provided target slice pointer (e.g. &[]bson.M or &[]MyStruct).
func (r *MongoResult) All(ctx context.Context, target any) error {
	if r.Cursor != nil {
		defer r.Cursor.Close(ctx)
		return r.Cursor.All(ctx, target)
	}
	if r.SingleResult != nil {
		return r.SingleResult.Decode(target)
	}
	return nil
}

// Documents returns a Go 1.23+ push iterator yielding each result document as bson.M.
func (r *MongoResult) Documents(ctx context.Context) iter.Seq2[bson.M, error] {
	return func(yield func(bson.M, error) bool) {
		if r.Cursor != nil {
			defer r.Cursor.Close(ctx)
			for r.Cursor.Next(ctx) {
				var doc bson.M
				if err := r.Cursor.Decode(&doc); err != nil {
					yield(nil, err)
					return
				}
				if !yield(doc, nil) {
					return
				}
			}
			if err := r.Cursor.Err(); err != nil {
				yield(nil, err)
			}
			return
		}

		if r.SingleResult != nil {
			var doc bson.M
			if err := r.SingleResult.Decode(&doc); err != nil {
				if !errors.Is(err, mongo.ErrNoDocuments) {
					yield(nil, err)
				}
				return
			}
			yield(doc, nil)
		}
	}
}

// Execute compiles and executes the plan against the configured MongoDB database.
func (b *MongoBackend) Execute(ctx context.Context, plan *Plan) (*MongoResult, error) {
	if err := b.Connect(ctx); err != nil {
		return nil, err
	}
	q, err := b.Compile(plan)
	if err != nil {
		return nil, err
	}
	return b.ExecuteQuery(ctx, q)
}

// ExecuteQuery executes a pre-compiled Query against the configured MongoDB database.
func (b *MongoBackend) ExecuteQuery(ctx context.Context, q *Query) (*MongoResult, error) {
	if b.config.DB == nil {
		if err := b.Connect(ctx); err != nil {
			return nil, err
		}
	}

	coll := b.config.DB.Collection(q.Collection)
	res := &MongoResult{Operation: q.Operation}

	switch q.Operation {
	case "find":
		findOpts := options.Find()
		if len(q.Projection) > 0 {
			findOpts.SetProjection(q.Projection)
		}
		if len(q.Sort) > 0 {
			findOpts.SetSort(q.Sort)
		}
		if q.Limit != nil {
			findOpts.SetLimit(*q.Limit)
		}
		if q.Offset != nil {
			findOpts.SetSkip(*q.Offset)
		}
		cur, err := coll.Find(ctx, q.Filter, findOpts)
		if err != nil {
			return nil, errnie.Error(err)
		}
		res.Cursor = cur
		return res, nil

	case "findone":
		findOneOpts := options.FindOne()
		if len(q.Projection) > 0 {
			findOneOpts.SetProjection(q.Projection)
		}
		if len(q.Sort) > 0 {
			findOneOpts.SetSort(q.Sort)
		}
		if q.Offset != nil {
			findOneOpts.SetSkip(*q.Offset)
		}
		res.SingleResult = coll.FindOne(ctx, q.Filter, findOneOpts)
		return res, nil

	case "count":
		countOpts := options.Count()
		if q.Limit != nil {
			countOpts.SetLimit(*q.Limit)
		}
		if q.Offset != nil {
			countOpts.SetSkip(*q.Offset)
		}
		count, err := coll.CountDocuments(ctx, q.Filter, countOpts)
		if err != nil {
			return nil, errnie.Error(err)
		}
		res.Count = count
		return res, nil

	case "distinct":
		fieldName := ""
		if len(q.Projection) > 0 {
			fieldName = q.Projection[0].Key
		}
		vals, err := coll.Distinct(ctx, fieldName, q.Filter)
		if err != nil {
			return nil, errnie.Error(err)
		}
		res.DistinctValues = vals
		return res, nil

	case "aggregate":
		cur, err := coll.Aggregate(ctx, q.Pipeline)
		if err != nil {
			return nil, errnie.Error(err)
		}
		res.Cursor = cur
		return res, nil

	default:
		return nil, errnie.Error(fmt.Errorf("unsupported MongoDB operation: %s", q.Operation))
	}
}

// CompileAny implements AnyBackend.
func (b *MongoBackend) CompileAny(plan *Plan) (any, error) {
	return b.Compile(plan)
}

// ExecuteAny implements AnyExecutor for dynamic execution dispatch.
func (b *MongoBackend) ExecuteAny(ctx context.Context, plan *Plan) (any, error) {
	return b.Execute(ctx, plan)
}

// Build implements QueryBuilder for populating an existing target structure.
func (b *MongoBackend) Build(plan *Plan, target any) (any, error) {
	if q, ok := target.(*Query); ok {
		return b.CompileWithQuery(plan, q)
	}
	return nil, errnie.Error(fmt.Errorf("unsupported build target type %T", target))
}

// Compile compiles an abstract Plan into a MongoDB Query.
func (b *MongoBackend) Compile(plan *Plan) (*Query, error) {
	q := NewQuery()
	return b.CompileWithQuery(plan, q)
}

// CompileWithQuery compiles an abstract Plan populating an existing Query object.
func (b *MongoBackend) CompileWithQuery(plan *Plan, q *Query) (*Query, error) {
	if plan == nil {
		return q, errnie.Error(fmt.Errorf("plan is nil"))
	}

	q.Collection = plan.Table.Name

	needsAggr := b.needsAggregation(plan)

	// Determine operation type
	if needsAggr {
		q.Operation = "aggregate"
	} else if plan.Distinct {
		q.Operation = "distinct"
	} else if plan.Limit != nil && *plan.Limit == 1 {
		q.Operation = "findone"
	} else if b.isSimpleCount(plan) {
		q.Operation = "count"
	} else {
		q.Operation = "find"
	}

	// Set Limit and Offset
	q.Limit = plan.Limit
	q.Offset = plan.Offset

	// Build Projections
	b.buildProjections(plan, q)

	// Build Filter (for find/findone/distinct/count)
	if plan.Where != nil {
		q.Filter = b.buildFilter(plan.Where, q.Collection)
	}

	// Build Aggregation Pipeline if needed
	if q.Operation == "aggregate" {
		b.buildPipeline(plan, q)
	}

	return q, nil
}

func (b *MongoBackend) isSimpleCount(plan *Plan) bool {
	if len(plan.Projections) != 1 {
		return false
	}
	proj := plan.Projections[0]
	if fn, ok := proj.Expr.(*FuncExpr); ok {
		if strings.EqualFold(fn.Name, "count") && !fn.Distinct && proj.Alias == "" {
			return true
		}
	}
	return false
}

func (b *MongoBackend) needsAggregation(plan *Plan) bool {
	if len(plan.GroupBy) > 0 || plan.Having != nil || len(plan.Joins) > 0 {
		return true
	}

	// Subqueries in projections
	for _, p := range plan.Projections {
		if _, ok := p.Expr.(*SubqueryExpr); ok {
			return true
		}
		if fn, ok := p.Expr.(*FuncExpr); ok {
			name := strings.ToLower(fn.Name)
			if name == "avg" || name == "sum" || name == "min" || name == "max" {
				return true
			}
			if name == "count" && (fn.Distinct || p.Alias != "") {
				return true
			}
		}
	}

	// Order by in combination with other aggregate features
	if len(plan.OrderBy) > 0 {
		for _, p := range plan.Projections {
			if _, ok := p.Expr.(*FuncExpr); ok {
				return true
			}
		}
	}

	return false
}

func (b *MongoBackend) buildProjections(plan *Plan, q *Query) {
	for _, p := range plan.Projections {
		if p.IsStar {
			q.Projection = nil
			return
		}
		if col, ok := p.Expr.(*ColumnExpr); ok {
			q.Projection = append(q.Projection, bson.E{Key: col.Name, Value: 1})
		} else if q.Operation == "distinct" {
			if fn, ok := p.Expr.(*FuncExpr); ok && len(fn.Args) > 0 {
				if col, ok := fn.Args[0].(*ColumnExpr); ok {
					q.Projection = append(q.Projection, bson.E{Key: col.Name, Value: 1})
				}
			}
		}
	}
}

func (b *MongoBackend) buildFilter(expr Expr, collection string) bson.D {
	filter := make(bson.D, 0)
	if expr == nil {
		return filter
	}

	switch e := expr.(type) {
	case *BinaryExpr:
		return b.buildBinaryFilter(e, collection)

	case *LogicalExpr:
		if strings.EqualFold(e.Operator, "AND") {
			for _, sub := range e.Exprs {
				subFilter := b.buildFilter(sub, collection)
				filter = append(filter, subFilter...)
			}
			return filter
		} else if strings.EqualFold(e.Operator, "OR") {
			var orList []bson.M
			for _, sub := range e.Exprs {
				subFilter := b.buildFilter(sub, collection)
				orList = append(orList, Map(subFilter))
			}
			return bson.D{{Key: "$or", Value: orList}}
		}

	case *RangeExpr:
		return bson.D{{
			Key: e.Field,
			Value: bson.M{
				"$gte": e.From,
				"$lte": e.To,
			},
		}}

	case *FuncExpr:
		if strings.EqualFold(e.Name, "array_contains") && len(e.Args) == 2 {
			field := ""
			if col, ok := e.Args[0].(*ColumnExpr); ok {
				field = col.Name
			}
			val := extractValue(e.Args[1])
			parsedVal := b.parseIDVal(val, collection)
			return bson.D{{
				Key:   field,
				Value: bson.M{"$in": []any{parsedVal}},
			}}
		}
	}

	return filter
}

func (b *MongoBackend) buildBinaryFilter(bExpr *BinaryExpr, collection string) bson.D {
	// Special case: ARRAY_CONTAINS on the left
	if fn, ok := bExpr.Left.(*FuncExpr); ok && strings.EqualFold(fn.Name, "array_contains") && len(fn.Args) == 2 {
		field := ""
		if col, ok := fn.Args[0].(*ColumnExpr); ok {
			field = col.Name
		}
		val := extractValue(fn.Args[1])
		parsedVal := b.parseIDVal(val, collection)
		return bson.D{{
			Key:   field,
			Value: bson.M{"$in": []any{parsedVal}},
		}}
	}

	field := ""
	if col, ok := bExpr.Left.(*ColumnExpr); ok {
		field = col.FullName()
	}

	val := extractValue(bExpr.Right)

	if isIDField(field, collection) {
		val = b.parseIDVal(val, collection)
	}

	op := strings.ToUpper(bExpr.Operator)
	switch op {
	case "=":
		return bson.D{{Key: field, Value: val}}
	case "!=":
		return bson.D{{Key: field, Value: bson.M{"$ne": val}}}
	case ">":
		return bson.D{{Key: field, Value: bson.M{"$gt": val}}}
	case ">=":
		return bson.D{{Key: field, Value: bson.M{"$gte": val}}}
	case "<":
		return bson.D{{Key: field, Value: bson.M{"$lt": val}}}
	case "<=":
		return bson.D{{Key: field, Value: bson.M{"$lte": val}}}
	case "LIKE":
		valStr := fmt.Sprintf("%v", val)
		regex := strings.ReplaceAll(strings.ReplaceAll(valStr, "%", ".*"), "_", ".")
		return bson.D{{Key: field, Value: bson.M{"$regex": regex, "$options": "i"}}}
	case "IN":
		vals := extractSlice(bExpr.Right)
		return bson.D{{Key: field, Value: bson.M{"$in": vals}}}
	}

	return bson.D{{Key: field, Value: val}}
}

func (b *MongoBackend) parseIDVal(val any, collection string) any {
	str, ok := val.(string)
	if !ok {
		return val
	}

	if mongoUUIDRegex.MatchString(str) {
		if collection != "" && unicode.IsUpper(rune(collection[0])) {
			if bin, err := CSUUID(str); err == nil {
				return bin
			}
		}
	}
	return str
}

func extractValue(expr Expr) any {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *LiteralExpr:
		return e.Value
	case *ColumnExpr:
		return e.FullName()
	case *ListExpr:
		items := make([]any, len(e.Elements))
		for i, el := range e.Elements {
			items[i] = extractValue(el)
		}
		return items
	default:
		return expr.String()
	}
}

func extractSlice(expr Expr) []any {
	if l, ok := expr.(*ListExpr); ok {
		res := make([]any, len(l.Elements))
		for i, el := range l.Elements {
			res[i] = extractValue(el)
		}
		return res
	}
	return []any{extractValue(expr)}
}

func (b *MongoBackend) buildPipeline(plan *Plan, q *Query) {
	hasJoins := len(plan.Joins) > 0

	// 1. Joins: $lookup / $unwind
	if hasJoins {
		for _, join := range plan.Joins {
			asName := join.Table.Name
			if join.Table.Alias != "" {
				asName = join.Table.Alias
			}
			lookupStage := bson.D{{
				Key: "$lookup",
				Value: bson.M{
					"from":         join.Table.Name,
					"localField":   join.LeftField,
					"foreignField": join.RightField,
					"as":           asName,
				},
			}}
			q.Pipeline = append(q.Pipeline, lookupStage)

			if strings.Contains(join.Type, "INNER") {
				q.Pipeline = append(q.Pipeline, bson.D{{
					Key:   "$unwind",
					Value: "$" + asName,
				}})
			}
		}
	}

	// 2. Subquery lookups in projections
	var subqueryProjections []Projection
	for _, proj := range plan.Projections {
		if _, ok := proj.Expr.(*SubqueryExpr); ok {
			subqueryProjections = append(subqueryProjections, proj)
		}
	}

	// 3. Match from WHERE
	if plan.Where != nil {
		matchFilter := b.buildFilter(plan.Where, q.Collection)
		if len(matchFilter) > 0 {
			q.Pipeline = append(q.Pipeline, bson.D{{
				Key:   "$match",
				Value: Map(matchFilter),
			}})
		}
	}

	// Subquery lookup stage (after match when no joins)
	for _, proj := range subqueryProjections {
		subExpr := proj.Expr.(*SubqueryExpr)
		subTable := subExpr.Plan.Table.Name
		q.Pipeline = append(q.Pipeline, bson.D{{
			Key: "$lookup",
			Value: bson.M{
				"from":         subTable,
				"localField":   "id",
				"foreignField": "user_id",
				"as":           subTable,
			},
		}})
	}

	// 4. Group stage (GROUP BY or aggregates)
	b.buildGroupStage(plan, q)

	// 5. Having / Match
	if plan.Having != nil {
		havingFilter := b.buildHavingFilter(plan.Having)
		if len(havingFilter) > 0 {
			q.Pipeline = append(q.Pipeline, bson.D{{
				Key:   "$match",
				Value: havingFilter,
			}})
		}
	}

	// 6. Project stage (if required)
	b.buildProjectStage(plan, q, subqueryProjections)

	// 7. Sort stage
	if len(plan.OrderBy) > 0 {
		sortDoc := bson.M{}
		for _, order := range plan.OrderBy {
			dir := 1
			if order.Direction == Desc {
				dir = -1
			}
			sortDoc[order.Field] = dir
		}
		q.Pipeline = append(q.Pipeline, bson.D{{
			Key:   "$sort",
			Value: sortDoc,
		}})
	}

	// 8. Limit stage
	if plan.Limit != nil {
		q.Pipeline = append(q.Pipeline, bson.D{{
			Key:   "$limit",
			Value: *plan.Limit,
		}})
	}
}

func (b *MongoBackend) buildGroupStage(plan *Plan, q *Query) {
	if len(plan.GroupBy) == 0 {
		hasAggr := false
		for _, p := range plan.Projections {
			if _, ok := p.Expr.(*FuncExpr); ok {
				hasAggr = true
				break
			}
		}
		if !hasAggr {
			return
		}

		groupStage := bson.M{"_id": nil}
		for _, p := range plan.Projections {
			if fn, ok := p.Expr.(*FuncExpr); ok {
				alias := p.Alias
				name := strings.ToLower(fn.Name)
				if name == "count" {
					if fn.Distinct && len(fn.Args) > 0 {
						if col, ok := fn.Args[0].(*ColumnExpr); ok {
							groupStage["temp"] = bson.M{"$addToSet": "$" + col.Name}
						}
					} else {
						if alias == "" {
							alias = "total"
						}
						groupStage[alias] = bson.M{"$sum": 1}
					}
				}
			}
		}
		q.Pipeline = append(q.Pipeline, bson.D{{Key: "$group", Value: groupStage}})
		return
	}

	// With GROUP BY
	groupStage := bson.M{}
	if len(plan.GroupBy) == 1 {
		field := plan.GroupBy[0]
		groupStage["_id"] = "$" + field
	} else {
		idMap := bson.M{}
		for _, field := range plan.GroupBy {
			idMap[field] = "$" + field
		}
		groupStage["_id"] = idMap
	}

	for _, field := range plan.GroupBy {
		groupStage[field] = bson.M{"$first": "$" + field}
	}

	for _, p := range plan.Projections {
		if fn, ok := p.Expr.(*FuncExpr); ok {
			alias := p.Alias
			name := strings.ToLower(fn.Name)
			if alias == "" {
				alias = name
			}

			if name == "count" {
				if fn.Distinct && len(fn.Args) > 0 {
					if col, ok := fn.Args[0].(*ColumnExpr); ok {
						groupStage[alias] = bson.M{"$addToSet": "$" + col.Name}
					}
				} else {
					groupStage[alias] = bson.M{"$sum": 1}
				}
			} else if len(fn.Args) > 0 {
				if col, ok := fn.Args[0].(*ColumnExpr); ok {
					groupStage[alias] = bson.M{"$" + name: "$" + col.Name}
				}
			}
		} else if col, ok := p.Expr.(*ColumnExpr); ok {
			groupStage[col.Name] = bson.M{"$first": "$" + col.Name}
		}
	}

	q.Pipeline = append(q.Pipeline, bson.D{{Key: "$group", Value: groupStage}})
}

func (b *MongoBackend) buildProjectStage(plan *Plan, q *Query, subqueries []Projection) {
	if len(subqueries) > 0 {
		projectStage := bson.M{}
		for _, p := range plan.Projections {
			if col, ok := p.Expr.(*ColumnExpr); ok {
				projectStage[col.Name] = 1
			} else if _, ok := p.Expr.(*SubqueryExpr); ok {
				projectStage[p.Alias] = bson.M{"$size": "$" + p.Alias}
			}
		}
		q.Pipeline = append(q.Pipeline, bson.D{{Key: "$project", Value: projectStage}})
		return
	}

	// Distinct count projection
	for _, p := range plan.Projections {
		if fn, ok := p.Expr.(*FuncExpr); ok && fn.Distinct && strings.EqualFold(fn.Name, "count") {
			projectStage := bson.M{}
			if len(plan.GroupBy) == 0 {
				projectStage[p.Alias] = bson.M{"$size": "$temp"}
			} else {
				for _, g := range plan.GroupBy {
					projectStage[g] = 1
				}
				for _, other := range plan.Projections {
					if other != p && other.Alias != "" {
						projectStage[other.Alias] = 1
					}
				}
				projectStage[p.Alias] = bson.M{"$size": "$" + p.Alias}
			}
			q.Pipeline = append(q.Pipeline, bson.D{{Key: "$project", Value: projectStage}})
			return
		}
	}
}

func (b *MongoBackend) buildHavingFilter(expr Expr) bson.M {
	havingDoc := bson.M{}
	if bExpr, ok := expr.(*BinaryExpr); ok {
		field := ""
		if fn, ok := bExpr.Left.(*FuncExpr); ok {
			if len(fn.Args) > 0 {
				if col, ok := fn.Args[0].(*ColumnExpr); ok {
					field = fn.Name + "_" + col.Name
				}
			} else {
				field = fn.Name
			}
		} else if col, ok := bExpr.Left.(*ColumnExpr); ok {
			field = col.Name
		}

		val := extractValue(bExpr.Right)
		op := "$" + mongoOp(bExpr.Operator)
		havingDoc[field] = bson.M{op: val}
	}
	return havingDoc
}

func mongoOp(op string) string {
	switch strings.ToUpper(op) {
	case ">":
		return "gt"
	case ">=":
		return "gte"
	case "<":
		return "lt"
	case "<=":
		return "lte"
	case "!=":
		return "ne"
	default:
		return "eq"
	}
}

func isIDField(field string, collection string) bool {
	return field == "_id" ||
		strings.HasSuffix(field, "Id") ||
		(field == "Accounts" && collection != "" && unicode.IsUpper(rune(collection[0])))
}

// Map converts a bson.D slice into a bson.M map.
func Map(d bson.D) bson.M {
	m := bson.M{}
	for _, e := range d {
		m[e.Key] = e.Value
	}
	return m
}

// CSUUID converts a UUID string to a MongoDB Binary type with subtype 3.
func CSUUID(uuid string) (primitive.Binary, error) {
	cleanUUID := strings.NewReplacer("{", "", "}", "", "-", "").Replace(uuid)

	reordered := cleanUUID[6:8] + cleanUUID[4:6] + cleanUUID[2:4] + cleanUUID[0:2] +
		cleanUUID[10:12] + cleanUUID[8:10] +
		cleanUUID[14:16] + cleanUUID[12:14] +
		cleanUUID[16:]

	rawBytes, err := hex.DecodeString(reordered)
	if err != nil {
		return primitive.Binary{}, errnie.Error(err)
	}

	buf := make([]byte, b64.StdEncoding.EncodedLen(len(rawBytes)))
	b64.StdEncoding.Encode(buf, rawBytes)

	out, err := b64.StdEncoding.DecodeString(strings.ReplaceAll(string(buf), " ", "+"))
	return primitive.Binary{
		Subtype: 3,
		Data:    out,
	}, errnie.Error(err)
}
