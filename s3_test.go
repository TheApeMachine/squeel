package squeel

import (
	"context"
	"fmt"
	"sort"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func setupRadixTestData() *RadixS3Client {
	mock := NewRadixS3Client()

	// 1. symm dataset (Flat partitioning: epoch/label/source/seq.bin)
	mock.Set("symm/1713000000/cvd/kraken/1.bin", []byte(`{"epoch":1713000000,"label":"cvd","source":"kraken","seq":1}`))
	mock.Set("symm/1713000000/cvd/kraken/2.bin", []byte(`{"epoch":1713000000,"label":"cvd","source":"kraken","seq":2}`))
	mock.Set("symm/1713000000/cvd/kraken/10.bin", []byte(`{"epoch":1713000000,"label":"cvd","source":"kraken","seq":10}`))
	mock.Set("symm/1713000000/cvd/coinbase/1.bin", []byte(`{"epoch":1713000000,"label":"cvd","source":"coinbase","seq":1}`))
	mock.Set("symm/1713000000/trades/kraken/1.bin", []byte(`{"epoch":1713000000,"label":"trades","source":"kraken","seq":1}`))
	mock.Set("symm/1713000000/depthflow/kraken/1.bin", []byte(`{"epoch":1713000000,"label":"depthflow","source":"kraken","seq":1}`))
	mock.Set("symm/1713000001/cvd/kraken/1.bin", []byte(`{"epoch":1713000001,"label":"cvd","source":"kraken","seq":1}`))
	mock.Set("symm/1713000001/trades/kraken/1.bin", []byte(`{"epoch":1713000001,"label":"trades","source":"kraken","seq":1}`))
	mock.Set("symm/1713000002/cvd/coinbase/1.bin", []byte(`{"epoch":1713000002,"label":"cvd","source":"coinbase","seq":1}`))

	// 2. products dataset (Hive partitioning: category=X)
	mock.Set("products/category=Electronics/p1.json", []byte(`{"id":1,"name":"Phone","price":500}`))
	mock.Set("products/category=Electronics/p2.json", []byte(`{"id":2,"name":"Laptop","price":1200}`))
	mock.Set("products/category=Books/p3.json", []byte(`{"id":3,"name":"Go in Action","price":40}`))
	mock.Set("products/category=Books/p4.json", []byte(`{"id":4,"name":"Concurrency in Go","price":45}`))
	mock.Set("products/category=Clothing/p5.json", []byte(`{"id":5,"name":"T-Shirt","price":20}`))

	// 3. events dataset (Hive composite: year=X/month=Y/day=Z)
	mock.Set("events/year=2024/month=10/day=14/part_0.bin", []byte("e1"))
	mock.Set("events/year=2024/month=10/day=14/part_1.bin", []byte("e2"))
	mock.Set("events/year=2024/month=10/day=15/part_0.bin", []byte("e3"))
	mock.Set("events/year=2024/month=11/day=01/part_0.bin", []byte("e4"))
	mock.Set("events/year=2024/month=12/day=25/part_0.bin", []byte("e5"))
	mock.Set("events/year=2025/month=01/day=01/part_0.bin", []byte("e6"))

	// 4. employees dataset (Hive date range: hire_date=YYYY-MM-DD)
	mock.Set("employees/hire_date=2020-01-01/e1.json", []byte(`{"id":1,"name":"Alice","salary":90000}`))
	mock.Set("employees/hire_date=2020-01-02/e2.json", []byte(`{"id":2,"name":"Bob","salary":80000}`))
	mock.Set("employees/hire_date=2020-01-03/e3.json", []byte(`{"id":3,"name":"Charlie","salary":85000}`))
	mock.Set("employees/hire_date=2020-01-04/e4.json", []byte(`{"id":4,"name":"David","salary":95000}`))
	mock.Set("employees/hire_date=2020-01-10/e5.json", []byte(`{"id":5,"name":"Eve","salary":110000}`))

	// 5. logs dataset (Auto-detected partitions)
	mock.Set("logs/env=production/service=auth/app.log", []byte("auth prod log"))
	mock.Set("logs/env=production/service=billing/app.log", []byte("billing prod log"))
	mock.Set("logs/env=staging/service=auth/app.log", []byte("auth staging log"))

	// 6. users dataset (Unpartitioned base table)
	mock.Set("users/u1.json", []byte(`{"id":1,"name":"User1"}`))
	mock.Set("users/u2.json", []byte(`{"id":2,"name":"User2"}`))
	mock.Set("users/u3.json", []byte(`{"id":3,"name":"User3"}`))

	return mock
}

type s3QueryTestCase struct {
	Name             string
	SQL              string
	Options          []S3Option
	ExpectedError    string
	ExpectedPrefixes []string
	ExpectedKeys     []string
	ExpectedResidual []string
}

var s3TestCases = []s3QueryTestCase{
	// ---------------------------------------------------------
	// Negative / Invalid Query Tests
	// ---------------------------------------------------------
	{
		Name:          "Invalid SQL syntax error",
		SQL:           "SELECT NOT VALID SYNTAX !!! FROM",
		ExpectedError: "syntax error",
	},
	{
		Name:          "Non-SELECT statement rejected",
		SQL:           "INSERT INTO users VALUES (1, 'alice')",
		ExpectedError: "unsupported SQL statement type",
	},
	{
		Name:          "Empty SQL statement rejected",
		SQL:           "",
		ExpectedError: "syntax error",
	},

	// ---------------------------------------------------------
	// Flat Partitioning Tests (Symm dataset: epoch/label/source)
	// ---------------------------------------------------------
	{
		Name: "Flat single partition key equality (epoch)",
		SQL:  "SELECT * FROM symm WHERE epoch = 1713000000",
		Options: []S3Option{
			WithPartitionStyle(FlatPartitioning),
			WithPartitionKeys("symm", "epoch", "label", "source"),
		},
		ExpectedPrefixes: []string{"symm/1713000000/"},
		ExpectedKeys: []string{
			"symm/1713000000/cvd/coinbase/1.bin",
			"symm/1713000000/cvd/kraken/1.bin",
			"symm/1713000000/cvd/kraken/2.bin",
			"symm/1713000000/cvd/kraken/10.bin",
			"symm/1713000000/depthflow/kraken/1.bin",
			"symm/1713000000/trades/kraken/1.bin",
		},
	},
	{
		Name: "Flat two-level partition hierarchy (epoch and label)",
		SQL:  "SELECT * FROM symm WHERE epoch = 1713000000 AND label = 'cvd'",
		Options: []S3Option{
			WithPartitionStyle(FlatPartitioning),
			WithPartitionKeys("symm", "epoch", "label", "source"),
		},
		ExpectedPrefixes: []string{"symm/1713000000/cvd/"},
		ExpectedKeys: []string{
			"symm/1713000000/cvd/coinbase/1.bin",
			"symm/1713000000/cvd/kraken/1.bin",
			"symm/1713000000/cvd/kraken/2.bin",
			"symm/1713000000/cvd/kraken/10.bin",
		},
	},
	{
		Name: "Flat three-level full partition hierarchy (epoch, label, source)",
		SQL:  "SELECT * FROM symm WHERE epoch = 1713000000 AND label = 'cvd' AND source = 'kraken'",
		Options: []S3Option{
			WithPartitionStyle(FlatPartitioning),
			WithPartitionKeys("symm", "epoch", "label", "source"),
		},
		ExpectedPrefixes: []string{"symm/1713000000/cvd/kraken/"},
		ExpectedKeys: []string{
			"symm/1713000000/cvd/kraken/1.bin",
			"symm/1713000000/cvd/kraken/2.bin",
			"symm/1713000000/cvd/kraken/10.bin",
		},
	},
	{
		Name: "Flat partition key with IN clause produces multiple prefix branches",
		SQL:  "SELECT * FROM symm WHERE epoch = 1713000000 AND label IN ('cvd', 'trades')",
		Options: []S3Option{
			WithPartitionStyle(FlatPartitioning),
			WithPartitionKeys("symm", "epoch", "label", "source"),
		},
		ExpectedPrefixes: []string{
			"symm/1713000000/cvd/",
			"symm/1713000000/trades/",
		},
		ExpectedKeys: []string{
			"symm/1713000000/cvd/coinbase/1.bin",
			"symm/1713000000/cvd/kraken/1.bin",
			"symm/1713000000/cvd/kraken/2.bin",
			"symm/1713000000/cvd/kraken/10.bin",
			"symm/1713000000/trades/kraken/1.bin",
		},
	},
	{
		Name: "Flat partition key with disjunctive OR condition",
		SQL:  "SELECT * FROM symm WHERE epoch = 1713000000 AND (label = 'trades' OR label = 'depthflow')",
		Options: []S3Option{
			WithPartitionStyle(FlatPartitioning),
			WithPartitionKeys("symm", "epoch", "label", "source"),
		},
		ExpectedPrefixes: []string{
			"symm/1713000000/trades/",
			"symm/1713000000/depthflow/",
		},
		ExpectedKeys: []string{
			"symm/1713000000/depthflow/kraken/1.bin",
			"symm/1713000000/trades/kraken/1.bin",
		},
	},
	{
		Name: "Flat partition key non-matching epoch yields empty result",
		SQL:  "SELECT * FROM symm WHERE epoch = 9999999999",
		Options: []S3Option{
			WithPartitionStyle(FlatPartitioning),
			WithPartitionKeys("symm", "epoch", "label", "source"),
		},
		ExpectedPrefixes: []string{"symm/9999999999/"},
		ExpectedKeys:     []string{},
	},

	// ---------------------------------------------------------
	// Hive Partitioning Tests (Products dataset: category=X)
	// ---------------------------------------------------------
	{
		Name: "Hive single partition equality",
		SQL:  "SELECT id, name, price FROM products WHERE category = 'Electronics'",
		Options: []S3Option{
			WithPartitionKeys("products", "category"),
		},
		ExpectedPrefixes: []string{"products/category=Electronics/"},
		ExpectedKeys: []string{
			"products/category=Electronics/p1.json",
			"products/category=Electronics/p2.json",
		},
	},
	{
		Name: "Hive partition key with IN clause",
		SQL:  "SELECT id, name FROM products WHERE category IN ('Electronics', 'Books')",
		Options: []S3Option{
			WithPartitionKeys("products", "category"),
		},
		ExpectedPrefixes: []string{
			"products/category=Electronics/",
			"products/category=Books/",
		},
		ExpectedKeys: []string{
			"products/category=Books/p3.json",
			"products/category=Books/p4.json",
			"products/category=Electronics/p1.json",
			"products/category=Electronics/p2.json",
		},
	},
	{
		Name: "Hive partition key with residual filter (non-partition column)",
		SQL:  "SELECT * FROM products WHERE category = 'Books' AND price > 42",
		Options: []S3Option{
			WithPartitionKeys("products", "category"),
		},
		ExpectedPrefixes: []string{"products/category=Books/"},
		ExpectedResidual: []string{"price"},
		ExpectedKeys: []string{
			"products/category=Books/p3.json",
			"products/category=Books/p4.json",
		},
	},
	{
		Name: "Hive partition key with multiple residual predicates",
		SQL:  "SELECT name, price FROM products WHERE category = 'Electronics' AND price > 100 AND in_stock = 1",
		Options: []S3Option{
			WithPartitionKeys("products", "category"),
		},
		ExpectedPrefixes: []string{"products/category=Electronics/"},
		ExpectedResidual: []string{"price", "in_stock"},
		ExpectedKeys: []string{
			"products/category=Electronics/p1.json",
			"products/category=Electronics/p2.json",
		},
	},

	// ---------------------------------------------------------
	// Composite Multi-Level Hive Tests (Events dataset: year/month/day)
	// ---------------------------------------------------------
	{
		Name: "Composite partition: all levels specified (year, month, day)",
		SQL:  "SELECT * FROM events WHERE year = 2024 AND month = 10 AND day = 14",
		Options: []S3Option{
			WithPartitionKeys("events", "year", "month", "day"),
		},
		ExpectedPrefixes: []string{"events/year=2024/month=10/day=14/"},
		ExpectedKeys: []string{
			"events/year=2024/month=10/day=14/part_0.bin",
			"events/year=2024/month=10/day=14/part_1.bin",
		},
	},
	{
		Name: "Composite partition: partial levels specified (year and month)",
		SQL:  "SELECT * FROM events WHERE year = 2024 AND month = 10",
		Options: []S3Option{
			WithPartitionKeys("events", "year", "month", "day"),
		},
		ExpectedPrefixes: []string{"events/year=2024/month=10/"},
		ExpectedKeys: []string{
			"events/year=2024/month=10/day=14/part_0.bin",
			"events/year=2024/month=10/day=14/part_1.bin",
			"events/year=2024/month=10/day=15/part_0.bin",
		},
	},
	{
		Name: "Composite partition: top-level only (year)",
		SQL:  "SELECT * FROM events WHERE year = 2024",
		Options: []S3Option{
			WithPartitionKeys("events", "year", "month", "day"),
		},
		ExpectedPrefixes: []string{"events/year=2024/"},
		ExpectedKeys: []string{
			"events/year=2024/month=10/day=14/part_0.bin",
			"events/year=2024/month=10/day=14/part_1.bin",
			"events/year=2024/month=10/day=15/part_0.bin",
			"events/year=2024/month=11/day=01/part_0.bin",
			"events/year=2024/month=12/day=25/part_0.bin",
		},
	},
	{
		Name: "Composite partition: multi-level with IN clause at second level",
		SQL:  "SELECT * FROM events WHERE year = 2024 AND month IN (11, 12)",
		Options: []S3Option{
			WithPartitionKeys("events", "year", "month", "day"),
		},
		ExpectedPrefixes: []string{
			"events/year=2024/month=11/",
			"events/year=2024/month=12/",
		},
		ExpectedKeys: []string{
			"events/year=2024/month=11/day=01/part_0.bin",
			"events/year=2024/month=12/day=25/part_0.bin",
		},
	},

	// ---------------------------------------------------------
	// Date Range Expansion Tests (Employees dataset: hire_date=YYYY-MM-DD)
	// ---------------------------------------------------------
	{
		Name: "Date range partition expansion using BETWEEN",
		SQL:  "SELECT * FROM employees WHERE hire_date BETWEEN '2020-01-01' AND '2020-01-03'",
		Options: []S3Option{
			WithPartitionKeys("employees", "hire_date"),
		},
		ExpectedPrefixes: []string{
			"employees/hire_date=2020-01-01/",
			"employees/hire_date=2020-01-02/",
			"employees/hire_date=2020-01-03/",
		},
		ExpectedKeys: []string{
			"employees/hire_date=2020-01-01/e1.json",
			"employees/hire_date=2020-01-02/e2.json",
			"employees/hire_date=2020-01-03/e3.json",
		},
	},
	{
		Name: "Date range partition expansion using >= and <=",
		SQL:  "SELECT * FROM employees WHERE hire_date >= '2020-01-02' AND hire_date <= '2020-01-04'",
		Options: []S3Option{
			WithPartitionKeys("employees", "hire_date"),
		},
		ExpectedPrefixes: []string{
			"employees/hire_date=2020-01-02/",
			"employees/hire_date=2020-01-03/",
			"employees/hire_date=2020-01-04/",
		},
		ExpectedKeys: []string{
			"employees/hire_date=2020-01-02/e2.json",
			"employees/hire_date=2020-01-03/e3.json",
			"employees/hire_date=2020-01-04/e4.json",
		},
	},

	// ---------------------------------------------------------
	// Unpartitioned & Auto-Detected Partition Tests
	// ---------------------------------------------------------
	{
		Name:             "Unpartitioned table query falls back to table base prefix",
		SQL:              "SELECT * FROM users",
		ExpectedPrefixes: []string{"users/"},
		ExpectedKeys: []string{
			"users/u1.json",
			"users/u2.json",
			"users/u3.json",
		},
	},
	{
		Name: "Auto-detected partition key from equality condition",
		SQL:  "SELECT * FROM logs WHERE env = 'production'",
		Options: []S3Option{
			WithAutoDetectPartitions(true),
		},
		ExpectedPrefixes: []string{"logs/env=production/"},
		ExpectedKeys: []string{
			"logs/env=production/service=auth/app.log",
			"logs/env=production/service=billing/app.log",
		},
	},

	// ---------------------------------------------------------
	// Pagination: Limit and Offset Tests
	// ---------------------------------------------------------
	{
		Name:             "LIMIT restricts maximum downloaded objects from radix trie",
		SQL:              "SELECT * FROM users LIMIT 2",
		ExpectedPrefixes: []string{"users/"},
		ExpectedKeys: []string{
			"users/u1.json",
			"users/u2.json",
		},
	},
	{
		Name:             "OFFSET skips leading objects in radix trie iteration",
		SQL:              "SELECT * FROM users LIMIT 10 OFFSET 1",
		ExpectedPrefixes: []string{"users/"},
		ExpectedKeys: []string{
			"users/u2.json",
			"users/u3.json",
		},
	},
	{
		Name:             "LIMIT and OFFSET combined paginate radix trie objects",
		SQL:              "SELECT * FROM users LIMIT 1 OFFSET 1",
		ExpectedPrefixes: []string{"users/"},
		ExpectedKeys: []string{
			"users/u2.json",
		},
	},
	{
		Name: "Flat partitioned query with LIMIT restricts downloaded objects",
		SQL:  "SELECT * FROM symm WHERE epoch = 1713000000 LIMIT 2",
		Options: []S3Option{
			WithPartitionStyle(FlatPartitioning),
			WithPartitionKeys("symm", "epoch", "label", "source"),
		},
		ExpectedPrefixes: []string{"symm/1713000000/"},
		ExpectedKeys: []string{
			"symm/1713000000/cvd/coinbase/1.bin",
			"symm/1713000000/cvd/kraken/1.bin",
		},
	},
	{
		Name: "Skipping middle partition key filters out non-matching objects",
		SQL:  "SELECT * FROM symm WHERE epoch = 1713000000 AND source = 'kraken'",
		Options: []S3Option{
			WithPartitionStyle(FlatPartitioning),
			WithPartitionKeys("symm", "epoch", "label", "source"),
		},
		ExpectedPrefixes: []string{"symm/1713000000/"},
		ExpectedKeys: []string{
			"symm/1713000000/cvd/kraken/1.bin",
			"symm/1713000000/cvd/kraken/2.bin",
			"symm/1713000000/cvd/kraken/10.bin",
			"symm/1713000000/depthflow/kraken/1.bin",
			"symm/1713000000/trades/kraken/1.bin",
		},
	},
}

func TestS3RadixQueries(t *testing.T) {
	ctx := context.Background()

	Convey("Given an S3Backend backed by an immutable radix trie S3 mock", t, func() {
		radixClient := setupRadixTestData()

		for idx, tc := range s3TestCases {
			Convey(fmt.Sprintf("[%d] %s: %s", idx, tc.Name, tc.SQL), func() {
				stmt := NewStatement(tc.SQL)

				// Construct S3 backend wired with the radix mock client
				opts := append([]S3Option{WithS3Client(radixClient)}, tc.Options...)
				s3Backend := NewS3Backend(opts...)

				if tc.ExpectedError != "" {
					plan, err := stmt.Parse()
					if err != nil {
						So(err.Error(), ShouldContainSubstring, tc.ExpectedError)
						return
					}
					_, err = s3Backend.Compile(plan)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldContainSubstring, tc.ExpectedError)
					return
				}

				// 1. Compile abstract plan
				plan, err := stmt.Parse()
				So(err, ShouldBeNil)
				So(plan, ShouldNotBeNil)

				s3Query, err := s3Backend.Compile(plan)
				So(err, ShouldBeNil)
				So(s3Query, ShouldNotBeNil)

				// 2. Validate prefix pruning
				if len(tc.ExpectedPrefixes) > 0 {
					So(s3Query.Prefixes, ShouldResemble, tc.ExpectedPrefixes)
				}

				// 3. Validate residual predicates
				if len(tc.ExpectedResidual) > 0 {
					var actualResidual []string
					for _, pred := range s3Query.Predicates {
						actualResidual = append(actualResidual, pred.Column)
					}
					So(actualResidual, ShouldResemble, tc.ExpectedResidual)
				}

				// 4. Stream objects from radix trie mock
				var downloadedKeys []string
				for obj, streamErr := range s3Backend.StreamObjects(ctx, plan) {
					So(streamErr, ShouldBeNil)
					So(obj, ShouldNotBeNil)
					So(len(obj.Data), ShouldBeGreaterThan, 0)
					downloadedKeys = append(downloadedKeys, obj.Key)
				}
				sort.Strings(downloadedKeys)

				expectedSorted := make([]string, len(tc.ExpectedKeys))
				copy(expectedSorted, tc.ExpectedKeys)
				sort.Strings(expectedSorted)

				if len(expectedSorted) == 0 {
					So(len(downloadedKeys), ShouldEqual, 0)
				} else {
					So(downloadedKeys, ShouldResemble, expectedSorted)
				}

				// 5. Test polymorphism: Statement.Stream() yields same byte stream count
				stmt.WithBackend(s3Backend)
				byteChunksCount := 0
				for chunk, streamErr := range stmt.Stream(ctx) {
					So(streamErr, ShouldBeNil)
					So(len(chunk), ShouldBeGreaterThan, 0)
					byteChunksCount++
				}
				So(byteChunksCount, ShouldEqual, len(tc.ExpectedKeys))
			})
		}
	})
}

func TestS3OrderedReplayAndPrefetch(t *testing.T) {
	ctx := context.Background()
	radixClient := setupRadixTestData()

	Convey("Given an S3Backend configured for hardware-speed ordered replay", t, func() {
		Convey("1. Global sequence ordering interleaves across prefixes and sorts naturally", func() {
			sql := "SELECT * FROM symm WHERE epoch = 1713000000 AND label IN ('cvd', 'trades') AND source = 'kraken'"
			stmt := NewStatement(sql)
			s3Backend := NewS3Backend(
				WithS3Client(radixClient),
				WithPartitionStyle(FlatPartitioning),
				WithPartitionKeys("symm", "epoch", "label", "source"),
				WithPrefetchConcurrency(16),
			)

			plan, err := stmt.Parse()
			So(err, ShouldBeNil)

			var receivedKeys []string
			for obj, streamErr := range s3Backend.StreamObjects(ctx, plan) {
				So(streamErr, ShouldBeNil)
				receivedKeys = append(receivedKeys, obj.Key)
			}

			// Validate exact chronological sequence ordering WITHOUT calling sort.Strings:
			// seq 1 across prefixes (cvd, then trades) must precede seq 2 (cvd/2.bin),
			// and seq 2 must precede seq 10 (cvd/10.bin)
			expectedOrder := []string{
				"symm/1713000000/cvd/kraken/1.bin",
				"symm/1713000000/trades/kraken/1.bin",
				"symm/1713000000/cvd/kraken/2.bin",
				"symm/1713000000/cvd/kraken/10.bin",
			}
			So(receivedKeys, ShouldResemble, expectedOrder)
		})

		Convey("2. ORDER BY seq DESC yields keys in reverse chronological sequence order", func() {
			sql := "SELECT * FROM symm WHERE epoch = 1713000000 AND label = 'cvd' AND source = 'kraken' ORDER BY seq DESC"
			stmt := NewStatement(sql)
			s3Backend := NewS3Backend(
				WithS3Client(radixClient),
				WithPartitionStyle(FlatPartitioning),
				WithPartitionKeys("symm", "epoch", "label", "source"),
				WithPrefetchConcurrency(8),
			)

			plan, err := stmt.Parse()
			So(err, ShouldBeNil)

			var receivedKeys []string
			for obj, streamErr := range s3Backend.StreamObjects(ctx, plan) {
				So(streamErr, ShouldBeNil)
				receivedKeys = append(receivedKeys, obj.Key)
			}

			expectedDesc := []string{
				"symm/1713000000/cvd/kraken/10.bin",
				"symm/1713000000/cvd/kraken/2.bin",
				"symm/1713000000/cvd/kraken/1.bin",
			}
			So(receivedKeys, ShouldResemble, expectedDesc)
		})

		Convey("3. Missing middle partition key filters out non-matching deeper partition levels", func() {
			// Partition hierarchy is (epoch, label, source).
			// Middle key 'label' is omitted in WHERE, while 'source = kraken' is specified.
			sql := "SELECT * FROM symm WHERE epoch = 1713000000 AND source = 'kraken'"
			stmt := NewStatement(sql)
			s3Backend := NewS3Backend(
				WithS3Client(radixClient),
				WithPartitionStyle(FlatPartitioning),
				WithPartitionKeys("symm", "epoch", "label", "source"),
			)

			plan, err := stmt.Parse()
			So(err, ShouldBeNil)

			var receivedKeys []string
			for obj, streamErr := range s3Backend.StreamObjects(ctx, plan) {
				So(streamErr, ShouldBeNil)
				receivedKeys = append(receivedKeys, obj.Key)
			}

			// Ensure coinbase is NEVER downloaded
			for _, k := range receivedKeys {
				So(k, ShouldNotContainSubstring, "coinbase")
				So(k, ShouldContainSubstring, "kraken")
			}
			So(len(receivedKeys), ShouldEqual, 5)
		})

		Convey("4. Bounded sliding-window prefetch pipeline yields identical payload data", func() {
			sql := "SELECT * FROM symm WHERE epoch = 1713000000 AND label = 'cvd' AND source = 'kraken'"
			stmt := NewStatement(sql)
			s3Backend := NewS3Backend(
				WithS3Client(radixClient),
				WithPartitionStyle(FlatPartitioning),
				WithPartitionKeys("symm", "epoch", "label", "source"),
				WithPrefetchConcurrency(2), // Test small sliding window constraint
			)

			plan, err := stmt.Parse()
			So(err, ShouldBeNil)

			count := 0
			for obj, streamErr := range s3Backend.StreamObjects(ctx, plan) {
				So(streamErr, ShouldBeNil)
				So(len(obj.Data), ShouldBeGreaterThan, 0)
				So(string(obj.Data), ShouldContainSubstring, `"source":"kraken"`)
				count++
			}
			So(count, ShouldEqual, 3)
		})
	})
}

