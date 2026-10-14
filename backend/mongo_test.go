package backend_test

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/theapemachine/squeel/backend"
)

func TestMongoBackend(t *testing.T) {
	Convey("Given a MongoBackend adapter", t, func() {
		mongo := backend.NewMongo()
		So(mongo.Name(), ShouldEqual, "mongodb")

		Convey("Compiling a simple plan generates find operation", func() {
			plan := backend.NewPlan("SELECT * FROM users")
			plan.Table = backend.TableRef{Name: "users"}
			plan.Projections = append(plan.Projections, backend.Projection{
				Expr:   &backend.StarExpr{},
				IsStar: true,
			})

			q, err := mongo.Compile(plan)
			So(err, ShouldBeNil)
			So(q.Collection, ShouldEqual, "users")
			So(q.Operation, ShouldEqual, "find")
			So(q.Projection, ShouldBeNil)
		})

		Convey("Nil plan returns error", func() {
			_, err := mongo.Compile(nil)
			So(err, ShouldNotBeNil)
		})

		Convey("Connection configuration options are applied", func() {
			b := backend.NewMongo(
				backend.WithMongoURI("mongodb://localhost:27017"),
				backend.WithMongoDatabase("testdb"),
			)
			So(b.Name(), ShouldEqual, "mongodb")

			err := b.Connect(context.Background())
			So(err, ShouldBeNil)
			So(b.Client(), ShouldNotBeNil)
			So(b.Database(), ShouldNotBeNil)
			So(b.Database().Name(), ShouldEqual, "testdb")
			_ = b.Close(context.Background())
		})

		Convey("Connecting without Database name returns error", func() {
			b := backend.NewMongo(backend.WithMongoURI("mongodb://localhost:27017"))
			err := b.Connect(context.Background())
			So(err, ShouldNotBeNil)
		})

		Convey("Connecting without URI or client returns error", func() {
			b := backend.NewMongo()
			err := b.Connect(context.Background())
			So(err, ShouldNotBeNil)
		})
	})
}
