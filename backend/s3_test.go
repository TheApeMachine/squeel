package backend_test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/theapemachine/squeel"
	"github.com/theapemachine/squeel/backend"
)

func TestS3Backend(t *testing.T) {
	Convey("Given an S3Backend adapter", t, func() {
		Convey("1. Single partition key with equality condition (Hive style)", func() {
			s3Backend := backend.NewS3(
				backend.WithBucket("analytics-bucket"),
				backend.WithPartitionKeys("employees", "department"),
				backend.WithPartitionStyle(backend.HivePartitioning),
			)

			stmt := squeel.NewStatement("SELECT name, salary FROM employees WHERE department = 'Engineering' AND salary > 80000")
			plan, err := stmt.Parse()
			So(err, ShouldBeNil)

			s3Query, err := s3Backend.Compile(plan)
			So(err, ShouldBeNil)
			So(s3Query.Bucket, ShouldEqual, "analytics-bucket")
			So(s3Query.Table, ShouldEqual, "employees")
			So(s3Query.Prefixes, ShouldResemble, []string{"employees/department=Engineering/"})
			So(s3Query.Columns, ShouldResemble, []string{"name", "salary"})
			So(s3Query.IsStar, ShouldBeFalse)

			// Residual filter: salary > 80000 should remain
			So(s3Query.HasResidualFilters(), ShouldBeTrue)
			So(len(s3Query.Predicates), ShouldEqual, 1)
			So(s3Query.Predicates[0].Column, ShouldEqual, "salary")
			So(s3Query.Predicates[0].Operator, ShouldEqual, ">")

			// Check URI generation
			uris := s3Query.S3URIs()
			So(uris, ShouldResemble, []string{"s3://analytics-bucket/employees/department=Engineering/"})
		})

		Convey("2. Single partition key with equality condition (Flat style)", func() {
			s3Backend := backend.NewS3(
				backend.WithBucket("data-lake"),
				backend.WithPartitionKeys("employees", "department"),
				backend.WithPartitionStyle(backend.FlatPartitioning),
			)

			plan, _ := squeel.ParseSQL("SELECT * FROM employees WHERE department = 'Sales'")
			s3Query, err := s3Backend.Compile(plan)
			So(err, ShouldBeNil)
			So(s3Query.Prefixes, ShouldResemble, []string{"employees/Sales/"})
			So(s3Query.IsStar, ShouldBeTrue)
			So(s3Query.S3URIs(), ShouldResemble, []string{"s3://data-lake/employees/Sales/"})
		})

		Convey("3. Partition key with IN operator generates multiple prefixes", func() {
			s3Backend := backend.NewS3(
				backend.WithBucket("store-lake"),
				backend.WithPartitionKeys("products", "category"),
			)

			plan, _ := squeel.ParseSQL("SELECT id, name FROM products WHERE category IN ('Electronics', 'Books') AND price > 50")
			s3Query, err := s3Backend.Compile(plan)
			So(err, ShouldBeNil)
			So(len(s3Query.Prefixes), ShouldEqual, 2)
			So(s3Query.Prefixes, ShouldResemble, []string{
				"products/category=Electronics/",
				"products/category=Books/",
			})
			// price > 50 is residual
			So(len(s3Query.Predicates), ShouldEqual, 1)
			So(s3Query.Predicates[0].Column, ShouldEqual, "price")
		})

		Convey("4. Multi-level composite partitions (year/month/day)", func() {
			s3Backend := backend.NewS3(
				backend.WithBucket("events-lake"),
				backend.WithPartitionKeys("events", "year", "month", "day"),
			)

			Convey("All partition levels specified", func() {
				plan, _ := squeel.ParseSQL("SELECT * FROM events WHERE year = 2024 AND month = 10 AND day = 14")
				s3Query, err := s3Backend.Compile(plan)
				So(err, ShouldBeNil)
				So(s3Query.Prefixes, ShouldResemble, []string{
					"events/year=2024/month=10/day=14/",
				})
			})

			Convey("Partial partition levels specified (year and month)", func() {
				plan, _ := squeel.ParseSQL("SELECT * FROM events WHERE year = 2024 AND month = 10")
				s3Query, err := s3Backend.Compile(plan)
				So(err, ShouldBeNil)
				So(s3Query.Prefixes, ShouldResemble, []string{
					"events/year=2024/month=10/",
				})
			})

			Convey("Top-level partition only (year)", func() {
				plan, _ := squeel.ParseSQL("SELECT * FROM events WHERE year = 2024")
				s3Query, err := s3Backend.Compile(plan)
				So(err, ShouldBeNil)
				So(s3Query.Prefixes, ShouldResemble, []string{
					"events/year=2024/",
				})
			})

			Convey("Multi-level with IN clause at second level", func() {
				plan, _ := squeel.ParseSQL("SELECT * FROM events WHERE year = 2024 AND month IN (11, 12)")
				s3Query, err := s3Backend.Compile(plan)
				So(err, ShouldBeNil)
				So(s3Query.Prefixes, ShouldResemble, []string{
					"events/year=2024/month=11/",
					"events/year=2024/month=12/",
				})
			})
		})

		Convey("5. Disjunctive OR condition on partition key", func() {
			s3Backend := backend.NewS3(
				backend.WithBucket("retail-lake"),
				backend.WithPartitionKeys("products", "category"),
			)

			plan, _ := squeel.ParseSQL("SELECT * FROM products WHERE (category = 'Electronics' OR category = 'Accessories')")
			s3Query, err := s3Backend.Compile(plan)
			So(err, ShouldBeNil)
			So(s3Query.Prefixes, ShouldResemble, []string{
				"products/category=Electronics/",
				"products/category=Accessories/",
			})
		})

		Convey("6. Date range partition expansion", func() {
			s3Backend := backend.NewS3(
				backend.WithBucket("hr-lake"),
				backend.WithPartitionKeys("employees", "hire_date"),
			)

			plan, _ := squeel.ParseSQL("SELECT * FROM employees WHERE hire_date >= '2020-01-01' AND hire_date <= '2020-01-03'")
			s3Query, err := s3Backend.Compile(plan)
			So(err, ShouldBeNil)
			So(s3Query.Prefixes, ShouldResemble, []string{
				"employees/hire_date=2020-01-01/",
				"employees/hire_date=2020-01-02/",
				"employees/hire_date=2020-01-03/",
			})
		})

		Convey("7. Unpartitioned table query defaults to table base prefix", func() {
			s3Backend := backend.NewS3(
				backend.WithBucket("lake"),
			)

			plan, _ := squeel.ParseSQL("SELECT * FROM users WHERE active = 1")
			s3Query, err := s3Backend.Compile(plan)
			So(err, ShouldBeNil)
			So(s3Query.Prefixes, ShouldResemble, []string{"users/"})
			So(len(s3Query.Predicates), ShouldEqual, 1)
			So(s3Query.Predicates[0].Column, ShouldEqual, "active")
		})

		Convey("8. BasePrefix prepends namespace to all prefixes", func() {
			s3Backend := backend.NewS3(
				backend.WithBucket("lake"),
				backend.WithBasePrefix("raw/data"),
				backend.WithPartitionKeys("users", "country"),
			)

			plan, _ := squeel.ParseSQL("SELECT * FROM users WHERE country = 'NL'")
			s3Query, err := s3Backend.Compile(plan)
			So(err, ShouldBeNil)
			So(s3Query.Prefixes, ShouldResemble, []string{"raw/data/users/country=NL/"})
			So(s3Query.S3URIs(), ShouldResemble, []string{"s3://lake/raw/data/users/country=NL/"})
		})

		Convey("9. AutoDetectPartitions infers equality column as partition key", func() {
			s3Backend := backend.NewS3(
				backend.WithBucket("dynamic-lake"),
				backend.WithAutoDetectPartitions(true),
			)

			plan, _ := squeel.ParseSQL("SELECT * FROM logs WHERE env = 'production' AND level = 'error'")
			s3Query, err := s3Backend.Compile(plan)
			So(err, ShouldBeNil)
			So(len(s3Query.Prefixes), ShouldBeGreaterThanOrEqualTo, 1)
			So(s3Query.Prefixes[0], ShouldContainSubstring, "logs/")
			So(s3Query.Prefixes[0], ShouldContainSubstring, "env=production/")
		})

		Convey("10. Limit and Offset are captured in S3Query", func() {
			s3Backend := backend.NewS3(backend.WithBucket("lake"))
			plan, _ := squeel.ParseSQL("SELECT * FROM items LIMIT 100 OFFSET 20")
			s3Query, err := s3Backend.Compile(plan)
			So(err, ShouldBeNil)
			So(s3Query.Limit, ShouldNotBeNil)
			So(*s3Query.Limit, ShouldEqual, 100)
			So(s3Query.Offset, ShouldNotBeNil)
			So(*s3Query.Offset, ShouldEqual, 20)
		})

		Convey("11. Nil plan returns an error", func() {
			s3Backend := backend.NewS3()
			_, err := s3Backend.Compile(nil)
			So(err, ShouldNotBeNil)
		})
	})
}
