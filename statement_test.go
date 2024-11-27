package squeel

import (
	"fmt"
	"strings" // Import the strings package
	"testing"
	"unicode"

	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var err error

var uuidIn = "695FF995-5DC4-4FBE-B80C-2621360D578F"
var uuidBin, _ = CSUUID(uuidIn)

func makeUUID() primitive.Binary {
	var uuidBin primitive.Binary

	if uuidBin, err = CSUUID(uuidIn); err != nil {
		return primitive.Binary{}
	}

	return uuidBin
}

const (
	// MongoDB operators
	mongoLookup  = "$lookup"
	mongoMatch   = "$match"
	mongoProject = "$project"
	mongoGroup   = "$group"
	mongoFirst   = "$first"

	// Common field references
	refCategory   = "$category"
	refDepartment = "$department"
	refSalary     = "$salary"
	refPrice      = "$price"
)

var stmts = []map[string]interface{}{{
	"sql":   "SQL (MYSQL DIALECT) TO MONGO WITH OPTIMIZER, LET'S GO!",
	"error": "syntax error at position 4 near 'sql'",
}, {
	"sql":        "SELECT * FROM users",
	"error":      nil,
	"operation":  "find",
	"collection": "users",
}, {
	"sql":        "select * from user where _id = '" + uuidIn + "'",
	"error":      nil,
	"operation":  "find",
	"collection": "user",
	"filter":     bson.D{{Key: "_id", Value: uuidIn}}, // Should remain a string for lowercase collection names
}, {
	"sql":        "select * from User where _id = '" + uuidIn + "'",
	"error":      nil,
	"operation":  "find",
	"collection": "User",
	"filter":     bson.D{{Key: "_id", Value: makeUUID()}}, // Should be converted to Binary for uppercase collection names
}, {
	"sql":        "SELECT first_name FROM user_profile WHERE _id = '" + uuidIn + "'",
	"error":      nil,
	"operation":  "find",
	"collection": "user_profile",
	"projection": bson.D{{Key: "first_name", Value: 1}},
	"filter":     bson.D{{Key: "_id", Value: uuidIn}}, // Should remain a string
}, {
	"sql":        "SELECT * FROM fanchecks WHERE _id = '" + uuidIn + "' LIMIT 10 OFFSET 2",
	"error":      nil,
	"operation":  "find",
	"collection": "fanchecks",
	"filter":     bson.D{{Key: "_id", Value: uuidIn}}, // Should remain a string
	"limit":      int64(10),
	"offset":     int64(2),
}, {
	"sql":        "SELECT a.uuid FROM answers a LIMIT 13, 1",
	"error":      nil,
	"operation":  "findone",
	"collection": "answers",
	"projection": bson.D{{Key: "uuid", Value: 1}},
	"limit":      int64(1),
	"offset":     int64(13),
}, {
	"sql":        "SELECT * FROM fanchecks LIMIT 1",
	"error":      nil,
	"operation":  "findone",
	"collection": "fanchecks",
	"limit":      int64(1),
}, {
	"sql":        "SELECT COUNT(q.*) FROM questions AS q",
	"error":      nil,
	"operation":  "count",
	"collection": "questions",
}, {
	"sql":        "SELECT DISTINCT(theme) FROM questions WHERE theme != ''",
	"error":      nil,
	"operation":  "distinct",
	"collection": "questions",
	"projection": "theme",
}, {
	"sql":        "SELECT * FROM User WHERE ARRAY_CONTAINS(Accounts, '" + uuidIn + "')",
	"error":      nil,
	"operation":  "find",
	"collection": "User",
	"filter": bson.D{
		{Key: "Accounts", Value: bson.M{"$in": []interface{}{uuidBin}}},
	},
}, {
	"sql":        "SELECT * FROM questions WHERE theme = 'Erkenning & Waardering'",
	"error":      nil,
	"operation":  "find",
	"collection": "questions",
	"filter": bson.D{
		{Key: "theme", Value: "Erkenning & Waardering"},
	},
}, {
	"sql":        "SELECT GroupName FROM `Group`",
	"error":      nil,
	"operation":  "find",
	"collection": "Group",
}, {
	"sql":        "SELECT u.name, p.city FROM users u JOIN profiles p ON u.id = p.user_id WHERE u.age > 25",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "users",
	"pipeline": []bson.M{
		{mongoLookup: bson.M{
			"from":         "profiles",
			"localField":   "id",
			"foreignField": "user_id",
			"as":           "user_profile",
		}},
		{mongoMatch: bson.M{
			"age": bson.M{"$gt": 25},
		}},
		{mongoProject: bson.M{
			"name": 1,
			"city": "$user_profile.city",
		}},
	},
}, {
	"sql":        "SELECT u.name, COUNT(o.id) AS order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.age > 25 GROUP BY u.id HAVING COUNT(o.id) > 5 ORDER BY order_count DESC LIMIT 10",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "users",
	"pipeline": []bson.M{
		{mongoLookup: bson.M{
			"from":         "orders",
			"localField":   "id",
			"foreignField": "user_id",
			"as":           "orders",
		}},
		{mongoMatch: bson.M{"age": bson.M{"$gt": 25}}},
		{mongoGroup: bson.M{
			"_id":         "$id",
			"name":        bson.M{mongoFirst: "$name"},
			"order_count": bson.M{"$sum": bson.M{"$size": "$orders"}},
		}},
		{mongoMatch: bson.M{"order_count": bson.M{"$gt": 5}}},
		{"$sort": bson.M{"order_count": -1}},
		{"$limit": 10},
	},
}, {
	"sql":        "SELECT p.name, c.name AS category_name FROM products p INNER JOIN categories c ON p.category_id = c.id WHERE p.price > 100 AND c.name IN ('Electronics', 'Books') ORDER BY p.price DESC",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoLookup: bson.M{
			"from":         "categories",
			"localField":   "category_id",
			"foreignField": "id",
			"as":           "category",
		}},
		{"$unwind": "$category"},
		{mongoMatch: bson.M{
			"price":         bson.M{"$gt": 100},
			"category.name": bson.M{"$in": []string{"Electronics", "Books"}},
		}},
		{mongoProject: bson.M{
			"name":          1,
			"category_name": "$category.name",
			"price":         1,
		}},
		{"$sort": bson.M{"price": -1}},
	},
}, {
	"sql":        "SELECT department, AVG(salary) AS avg_salary FROM employees WHERE hire_date >= '2020-01-01' GROUP BY department HAVING AVG(salary) > 50000",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "employees",
	"pipeline": []bson.M{
		{mongoMatch: bson.M{"hire_date": bson.M{"$gte": "2020-01-01"}}},
		{mongoGroup: bson.M{
			"_id":        refDepartment,
			"avg_salary": bson.M{"$avg": refSalary},
		}},
		{mongoMatch: bson.M{"avg_salary": bson.M{"$gt": 50000}}},
		{mongoProject: bson.M{
			"department": refDepartment,
			"avg_salary": 1,
			"_id":        0,
		}},
	},
}, {
	"sql":        "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count FROM users u WHERE u.status = 'active'",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "users",
	"pipeline": []bson.M{
		{mongoMatch: bson.M{"status": "active"}},
		{mongoLookup: bson.M{
			"from":         "orders",
			"localField":   "id",
			"foreignField": "user_id",
			"as":           "orders",
		}},
		{mongoProject: bson.M{
			"name":        1,
			"order_count": bson.M{"$size": "$orders"},
		}},
	},
}, {
	"sql":        "SELECT * FROM products WHERE name LIKE '%phone%' AND (category = 'Electronics' OR category = 'Accessories') AND price BETWEEN 100 AND 500",
	"error":      nil,
	"operation":  "find",
	"collection": "products",
	"filter": bson.D{
		{Key: "name", Value: bson.M{"$regex": ".*phone.*", "$options": "i"}},
		{Key: "$or", Value: []bson.M{
			{"category": "Electronics"},
			{"category": "Accessories"},
		}},
		{Key: "price", Value: bson.M{"$gte": 100, "$lte": 500}},
	},
}, {
	"sql":        "SELECT * FROM questions WHERE theme.nl = 'Some Theme'",
	"error":      nil,
	"operation":  "find",
	"collection": "questions",
	"filter":     bson.D{{Key: "theme.nl", Value: "Some Theme"}},
}, {
	"sql":        "SELECT department, COUNT(*) as emp_count FROM employees GROUP BY department ORDER BY emp_count DESC",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "employees",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":        refDepartment,
			"emp_count":  bson.M{"$sum": 1},
			"department": bson.M{mongoFirst: refDepartment},
		}},
		{"$sort": bson.M{"emp_count": -1}},
	},
}, {
	"sql":        "SELECT category, AVG(price) as avg_price FROM products GROUP BY category HAVING avg_price > 100",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":       refCategory,
			"avg_price": bson.M{"$avg": refPrice},
			"category":  bson.M{mongoFirst: refCategory},
		}},
		{mongoMatch: bson.M{"avg_price": bson.M{"$gt": 100}}},
	},
}, {
	"sql":        "SELECT category, MIN(price) as min_price, MAX(price) as max_price, AVG(price) as avg_price FROM products GROUP BY category",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":       refCategory,
			"category":  bson.M{mongoFirst: refCategory},
			"min_price": bson.M{"$min": refPrice},
			"max_price": bson.M{"$max": refPrice},
			"avg_price": bson.M{"$avg": refPrice},
		}},
	},
}, {
	"sql":        "SELECT department, SUM(salary) as total_salary, COUNT(DISTINCT employee_id) as emp_count FROM payroll GROUP BY department HAVING total_salary > 1000000",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "payroll",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":          refDepartment,
			"department":   bson.M{mongoFirst: refDepartment},
			"total_salary": bson.M{"$sum": refSalary},
			"emp_count":    bson.M{"$addToSet": "$employee_id"},
		}},
		{mongoProject: bson.M{
			"department":   1,
			"total_salary": 1,
			"emp_count":    bson.M{"$size": "$emp_count"},
		}},
		{mongoMatch: bson.M{"total_salary": bson.M{"$gt": 1000000}}},
	},
}, {
	"sql":        "SELECT COUNT(*) as total FROM users",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "users",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":   nil,
			"total": bson.M{"$sum": 1},
		}},
	},
}, {
	"sql":        "SELECT COUNT(DISTINCT user_id) as unique_users FROM events",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "events",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":  nil,
			"temp": bson.M{"$addToSet": "$user_id"},
		}},
		{mongoProject: bson.M{
			"unique_users": bson.M{"$size": "$temp"},
		}},
	},
}, {
	"sql":        "SELECT department, MIN(salary) as min_sal, MAX(salary) as max_sal, AVG(salary) as avg_sal FROM employees GROUP BY department",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "employees",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":        refDepartment,
			"department": bson.M{mongoFirst: refDepartment},
			"min_sal":    bson.M{"$min": refSalary},
			"max_sal":    bson.M{"$max": refSalary},
			"avg_sal":    bson.M{"$avg": refSalary},
		}},
	},
}, // Add this comma
} // Close the outer slice

func TestSqueel(t *testing.T) {
	Convey("Given a Squeel statement", t, func() {
		for idx, stmt := range stmts {
			testCase := newTestCase(idx, stmt)
			testCase.run(t)
		}
	})
}

type testCase struct {
	idx  int
	stmt map[string]interface{}
	q    *Query
	err  error
	sql  string
}

func newTestCase(idx int, stmt map[string]interface{}) *testCase {
	return &testCase{
		idx:  idx,
		stmt: stmt,
		q:    NewQuery(),
		sql:  stmt["sql"].(string),
	}
}

func (tc *testCase) run(_ *testing.T) {
	Convey(fmt.Sprintf("[%d] %s", tc.idx, tc.sql), func() {
		tc.buildQuery()
		tc.testInvalidSQL()
		tc.testValidSQL()
	})
}

func (tc *testCase) buildQuery() {
	squeel := NewStatement(tc.sql)
	tc.q, tc.err = squeel.Build(tc.q)
}

func (tc *testCase) testInvalidSQL() {
	Convey(fmt.Sprintf("[%d] When invalid SQL statement", tc.idx), func() {
		if errMsg, ok := tc.stmt["error"].(string); ok {
			tc.assertError(errMsg)
		}
	})
}

func (tc *testCase) testValidSQL() {
	Convey(fmt.Sprintf("[%d] When valid SQL statement", tc.idx), func() {
		if _, ok := tc.stmt["error"].(string); !ok {
			tc.assertNoError()
			tc.assertOperation()
			tc.assertCollection()
			tc.assertFilter()
			tc.assertProjection()
			tc.assertLimitAndOffset()
		}
	})
}

func (tc *testCase) assertError(errMsg string) {
	Convey(fmt.Sprintf("[%d] should error with %s", tc.idx, errMsg), func() {
		So(tc.err, ShouldNotBeNil)
		So(tc.err.Error(), ShouldContainSubstring, errMsg)
	})
}

func (tc *testCase) assertNoError() {
	Convey(fmt.Sprintf("[%d] should not error", tc.idx), func() {
		So(tc.err, ShouldBeNil)
	})
}

func (tc *testCase) assertOperation() {
	if operation, ok := tc.stmt["operation"].(string); ok {
		Convey(fmt.Sprintf("[%d] should use %s", tc.idx, operation), func() {
			So(tc.q.Operation, ShouldEqual, operation)
		})
	}
}

func (tc *testCase) assertCollection() {
	if collection, ok := tc.stmt["collection"].(string); ok {
		Convey(fmt.Sprintf("[%d] should target [%s]", tc.idx, collection), func() {
			So(tc.q.Collection, ShouldEqual, collection)
		})
	}
}

func (tc *testCase) assertFilter() {
	if filter, ok := tc.stmt["filter"].(bson.D); ok {
		Convey(fmt.Sprintf("[%d] should filter %v", tc.idx, filter), func() {
			So(tc.q.Filter, ShouldResemble, filter)
			tc.assertIDHandling()
		})
	}
}

func (tc *testCase) assertIDHandling() {
	for _, f := range tc.q.Filter {
		if f.Key == "_id" || strings.HasSuffix(f.Key, "Id") {
			if unicode.IsUpper(rune(tc.q.Collection[0])) {
				So(f.Value, ShouldHaveSameTypeAs, primitive.Binary{})
			} else {
				So(f.Value, ShouldHaveSameTypeAs, "")
			}
		}
	}
}

func (tc *testCase) assertProjection() {
	if projection, ok := tc.stmt["projection"].(bson.D); ok {
		Convey(fmt.Sprintf("[%d] should project %v", tc.idx, projection), func() {
			So(tc.q.Projection, ShouldResemble, projection)
		})
	}
}

func (tc *testCase) assertLimitAndOffset() {
	tc.assertLimit()
	tc.assertOffset()
}

func (tc *testCase) assertLimit() {
	if limit, ok := tc.stmt["limit"].(int64); ok {
		Convey(fmt.Sprintf("[%d] should limit %d", tc.idx, limit), func() {
			So(*tc.q.Limit, ShouldEqual, limit)
		})
	}
}

func (tc *testCase) assertOffset() {
	if offset, ok := tc.stmt["offset"].(int64); ok {
		Convey(fmt.Sprintf("[%d] should offset %d", tc.idx, offset), func() {
			So(*tc.q.Offset, ShouldEqual, offset)
		})
	}
}
