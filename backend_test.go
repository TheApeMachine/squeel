package squeel

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDecoupledParserAndPlan(t *testing.T) {
	Convey("Given a SQL statement parsed with ParseSQL", t, func() {
		Convey("A simple SELECT query produces a storage-agnostic Plan", func() {
			plan, err := ParseSQL("SELECT first_name, last_name FROM users WHERE age > 21 ORDER BY last_name ASC LIMIT 50 OFFSET 10")
			So(err, ShouldBeNil)
			So(plan, ShouldNotBeNil)
			So(plan.Table.Name, ShouldEqual, "users")
			So(plan.Distinct, ShouldBeFalse)
			So(len(plan.Projections), ShouldEqual, 2)
			So(plan.Projections[0].Expr.String(), ShouldEqual, "first_name")
			So(plan.Projections[1].Expr.String(), ShouldEqual, "last_name")

			So(plan.Where, ShouldNotBeNil)
			binWhere, ok := plan.Where.(*BinaryExpr)
			So(ok, ShouldBeTrue)
			So(binWhere.Left.String(), ShouldEqual, "age")
			So(binWhere.Operator, ShouldEqual, ">")
			So(binWhere.Right.String(), ShouldEqual, "21")

			So(len(plan.OrderBy), ShouldEqual, 1)
			So(plan.OrderBy[0].Field, ShouldEqual, "last_name")
			So(plan.OrderBy[0].Direction, ShouldEqual, Asc)

			So(plan.Limit, ShouldNotBeNil)
			So(*plan.Limit, ShouldEqual, 50)
			So(plan.Offset, ShouldNotBeNil)
			So(*plan.Offset, ShouldEqual, 10)
		})

		Convey("A query with JOIN produces proper table references and ON conditions", func() {
			plan, err := ParseSQL("SELECT u.name, p.city FROM users u INNER JOIN profiles p ON u.id = p.user_id")
			So(err, ShouldBeNil)
			So(plan.Table.Name, ShouldEqual, "users")
			So(plan.Table.Alias, ShouldEqual, "u")
			So(len(plan.Joins), ShouldEqual, 1)
			So(plan.Joins[0].Table.Name, ShouldEqual, "profiles")
			So(plan.Joins[0].Table.Alias, ShouldEqual, "p")
			So(plan.Joins[0].LeftField, ShouldEqual, "id")
			So(plan.Joins[0].RightField, ShouldEqual, "user_id")
		})

		Convey("A query with GROUP BY and HAVING produces group and having expressions", func() {
			plan, err := ParseSQL("SELECT department, AVG(salary) FROM employees GROUP BY department HAVING AVG(salary) > 50000")
			So(err, ShouldBeNil)
			So(len(plan.GroupBy), ShouldEqual, 1)
			So(plan.GroupBy[0], ShouldEqual, "department")
			So(plan.Having, ShouldNotBeNil)
			binHaving, ok := plan.Having.(*BinaryExpr)
			So(ok, ShouldBeTrue)
			So(binHaving.Operator, ShouldEqual, ">")
		})
	})
}

func TestBackendAdapterPattern(t *testing.T) {
	Convey("Given different backend adapters", t, func() {
		stmt := NewStatement("SELECT name FROM products WHERE category = 'Electronics' LIMIT 10")
		plan, err := stmt.Parse()
		So(err, ShouldBeNil)

		Convey("MongoBackend compiles the plan into a MongoDB Query", func() {
			mongoBackend := NewMongoBackend()
			So(mongoBackend.Name(), ShouldEqual, "mongodb")

			q, err := mongoBackend.Compile(plan)
			So(err, ShouldBeNil)
			So(q, ShouldNotBeNil)
			So(q.Collection, ShouldEqual, "products")
			So(q.Operation, ShouldEqual, "find")
			So(*q.Limit, ShouldEqual, 10)
		})

		Convey("S3Backend compiles the plan into an S3Query", func() {
			s3Backend := NewS3Backend(
				WithBucket("lake"),
				WithPartitionKeys("products", "category"),
			)
			So(s3Backend.Name(), ShouldEqual, "s3")

			s3Query, err := s3Backend.Compile(plan)
			So(err, ShouldBeNil)
			So(s3Query, ShouldNotBeNil)
			So(s3Query.Bucket, ShouldEqual, "lake")
			So(s3Query.Table, ShouldEqual, "products")
			So(s3Query.Prefixes, ShouldResemble, []string{"products/category=Electronics/"})
			So(*s3Query.Limit, ShouldEqual, 10)
		})

		Convey("Compile generic helper works uniformly across backends", func() {
			mongoQ, err := Compile(stmt, NewMongoBackend())
			So(err, ShouldBeNil)
			So(mongoQ.Collection, ShouldEqual, "products")

			s3Q, err := Compile(stmt, NewS3Backend(WithPartitionKeys("products", "category")))
			So(err, ShouldBeNil)
			So(s3Q.Table, ShouldEqual, "products")
		})

		Convey("Statement polymorphic Compile works via AnyBackend interfaces", func() {
			stmtWithMongo := NewStatement("SELECT name FROM products WHERE category = 'Electronics' LIMIT 10", NewMongoBackend())
			mongoAny, err := stmtWithMongo.Compile()
			So(err, ShouldBeNil)
			mongoQ, ok := mongoAny.(*Query)
			So(ok, ShouldBeTrue)
			So(mongoQ.Collection, ShouldEqual, "products")

			stmtWithS3 := NewStatement("SELECT name FROM products WHERE category = 'Electronics' LIMIT 10")
			s3Any, err := stmtWithS3.Compile(NewS3Backend(WithPartitionKeys("products", "category")))
			So(err, ShouldBeNil)
			s3Q, ok := s3Any.(*S3Query)
			So(ok, ShouldBeTrue)
			So(s3Q.Table, ShouldEqual, "products")
			So(s3Q.Prefixes, ShouldResemble, []string{"products/category=Electronics/"})
		})
	})
}
