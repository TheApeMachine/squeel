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
}, {
	"sql":        "SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END AS age_group FROM users",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "users",
	"pipeline": []bson.M{
		{mongoProject: bson.M{
			"age_group": bson.M{
				"$cond": bson.M{
					"if":   bson.M{"$gt": []interface{}{"$age", 18}},
					"then": "adult",
					"else": "minor",
				},
			},
		}},
	},
}, {
	"sql":        "SELECT name, CASE category WHEN 'electronics' THEN price * 0.9 WHEN 'books' THEN price * 0.95 ELSE price END AS discounted_price FROM products",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoProject: bson.M{
			"name": 1,
			"discounted_price": bson.M{
				"$cond": bson.M{
					"if":   bson.M{"$eq": []interface{}{"$category", "electronics"}},
					"then": bson.M{"$multiply": []interface{}{"$price", 0.9}},
					"else": bson.M{
						"$cond": bson.M{
							"if":   bson.M{"$eq": []interface{}{"$category", "books"}},
							"then": bson.M{"$multiply": []interface{}{"$price", 0.95}},
							"else": "$price",
						},
					},
				},
			},
		}},
	},
}, {
	"sql":        "SELECT COUNT(DISTINCT user_id) as unique_users, AVG(amount) as avg_amount FROM orders WHERE status IN ('completed', 'shipped')",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "orders",
	"pipeline": []bson.M{
		{mongoMatch: bson.M{
			"status": bson.M{"$in": []string{"completed", "shipped"}},
		}},
		{mongoGroup: bson.M{
			"_id":          nil,
			"unique_users": bson.M{"$addToSet": "$user_id"},
			"avg_amount":   bson.M{"$avg": "$amount"},
		}},
		{mongoProject: bson.M{
			"unique_users": bson.M{"$size": "$unique_users"},
			"avg_amount":   1,
		}},
	},
}, {
	"sql":        "SELECT name, description FROM products WHERE name LIKE '%phone%' OR description LIKE '%mobile%'",
	"error":      nil,
	"operation":  "find",
	"collection": "products",
	"projection": bson.D{
		{Key: "name", Value: 1},
		{Key: "description", Value: 1},
	},
	"filter": bson.D{{
		Key: "$or",
		Value: []bson.M{
			{"name": bson.M{"$regex": ".*phone.*"}},
			{"description": bson.M{"$regex": ".*mobile.*"}},
		},
	}},
}, {
	"sql":        "SELECT department, SUM(CASE WHEN status = 'active' THEN salary ELSE 0 END) as active_salary FROM employees GROUP BY department",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "employees",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id": "$department",
			"active_salary": bson.M{
				"$sum": bson.M{
					"$cond": bson.M{
						"if":   bson.M{"$eq": []interface{}{"$status", "active"}},
						"then": "$salary",
						"else": 0,
					},
				},
			},
			"department": bson.M{mongoFirst: "$department"},
		}},
	},
}, {
	"sql":        "SELECT name, (price > 100 AND stock > 0) as in_stock_expensive FROM products",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoProject: bson.M{
			"name": 1,
			"in_stock_expensive": bson.M{
				"$and": []bson.M{
					{"$gt": []interface{}{"$price", 100}},
					{"$gt": []interface{}{"$stock", 0}},
				},
			},
		}},
	},
}, {
	"sql":        "SELECT * FROM orders WHERE created_at BETWEEN '2023-01-01' AND '2023-12-31' AND (status = 'pending' OR status = 'processing')",
	"error":      nil,
	"operation":  "find",
	"collection": "orders",
	"filter": bson.D{
		{Key: "created_at", Value: bson.M{
			"$gte": "2023-01-01",
			"$lte": "2023-12-31",
		}},
		{Key: "$or", Value: []bson.M{
			{"status": "pending"},
			{"status": "processing"},
		}},
	},
}, {
	"sql":        "SELECT COUNT(*) as count, SUM(price) as total, AVG(price) as avg, MIN(price) as min, MAX(price) as max FROM products",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":   nil,
			"count": bson.M{"$sum": 1},
			"total": bson.M{"$sum": "$price"},
			"avg":   bson.M{"$avg": "$price"},
			"min":   bson.M{"$min": "$price"},
			"max":   bson.M{"$max": "$price"},
		}},
	},
}, {
	"sql":        "SELECT COUNT(DISTINCT user_id) as unique_users FROM orders",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "orders",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":          nil,
			"unique_users": bson.M{"$addToSet": "$user_id"},
		}},
		{mongoProject: bson.M{
			"unique_users": bson.M{"$size": "$unique_users"},
		}},
	},
}, {
	"sql":        "SELECT * FROM products WHERE price > 100 AND quantity <= 50 AND category != 'books' AND supplier IN ('A', 'B') AND status NOT IN ('discontinued')",
	"error":      nil,
	"operation":  "find",
	"collection": "products",
	"filter": bson.D{
		{Key: "price", Value: bson.M{"$gt": 100}},
		{Key: "quantity", Value: bson.M{"$lte": 50}},
		{Key: "category", Value: bson.M{"$ne": "books"}},
		{Key: "supplier", Value: bson.M{"$in": []string{"A", "B"}}},
		{Key: "status", Value: bson.M{"$nin": []string{"discontinued"}}},
	},
}, {
	"sql":        "SELECT COUNT(*) as total FROM orders",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "orders",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":   nil,
			"total": bson.M{"$sum": 1},
		}},
	},
}, {
	"sql":        "SELECT COUNT(DISTINCT user_id) as unique_users FROM orders",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "orders",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":          nil,
			"unique_users": bson.M{"$addToSet": "$user_id"},
		}},
		{mongoProject: bson.M{
			"unique_users": bson.M{"$size": "$unique_users"},
		}},
	},
}, {
	"sql":        "SELECT SUM(price) as total_price FROM products",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":         nil,
			"total_price": bson.M{"$sum": "$price"},
		}},
	},
}, {
	"sql":        "SELECT AVG(price) as avg_price FROM products",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":       nil,
			"avg_price": bson.M{"$avg": "$price"},
		}},
	},
}, {
	"sql":        "SELECT MIN(price) as min_price FROM products",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":       nil,
			"min_price": bson.M{"$min": "$price"},
		}},
	},
}, {
	"sql":        "SELECT MAX(price) as max_price FROM products",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":       nil,
			"max_price": bson.M{"$max": "$price"},
		}},
	},
}, {
	"sql":        "SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary, MIN(salary) as min_salary, MAX(salary) as max_salary FROM employees GROUP BY department",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "employees",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":        "$department",
			"emp_count":  bson.M{"$sum": 1},
			"avg_salary": bson.M{"$avg": "$salary"},
			"min_salary": bson.M{"$min": "$salary"},
			"max_salary": bson.M{"$max": "$salary"},
		}},
	},
}, {
	"sql":        "SELECT COUNT(DISTINCT status) as unique_statuses, COUNT(*) as total_orders FROM orders",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "orders",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":             nil,
			"unique_statuses": bson.M{"$addToSet": "$status"},
			"total_orders":    bson.M{"$sum": 1},
		}},
		{mongoProject: bson.M{
			"unique_statuses": bson.M{"$size": "$unique_statuses"},
			"total_orders":    1,
		}},
	},
}, {
	"sql":        "SELECT UNKNOWN_FUNC(user_id) as bad_func FROM users",
	"error":      fmt.Errorf("unhandled function: UNKNOWN_FUNC"),
	"operation":  "aggregate",
	"collection": "users",
	"pipeline":   []bson.M{},
}, {
	"sql":        "SELECT COUNT(*) as total, SUM(price) as total_price, AVG(price) as avg_price, MIN(price) as min_price, MAX(price) as max_price FROM products",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":         nil,
			"total":       bson.M{"$sum": 1},
			"total_price": bson.M{"$sum": "$price"},
			"avg_price":   bson.M{"$avg": "$price"},
			"min_price":   bson.M{"$min": "$price"},
			"max_price":   bson.M{"$max": "$price"},
		}},
	},
}, {
	"sql":        "SELECT COUNT(DISTINCT status) as unique_statuses, COUNT(*) as total FROM orders",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "orders",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":             nil,
			"unique_statuses": bson.M{"$addToSet": "$status"},
			"total":           bson.M{"$sum": 1},
		}},
		{mongoProject: bson.M{
			"unique_statuses": bson.M{"$size": "$unique_statuses"},
			"total":           1,
		}},
	},
}, {
	"sql":        "SELECT UNKNOWN_FUNC(field) as bad_func FROM users",
	"error":      fmt.Errorf("unhandled function: UNKNOWN_FUNC"),
	"operation":  "aggregate",
	"collection": "users",
}, {
	"sql":        "SELECT COUNT(DISTINCT status) as unique_statuses, COUNT(*) as total_orders FROM orders",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "orders",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":             nil,
			"unique_statuses": bson.M{"$addToSet": "$status"},
			"total_orders":    bson.M{"$sum": 1},
		}},
		{mongoProject: bson.M{
			"unique_statuses": bson.M{"$size": "$unique_statuses"},
			"total_orders":    1,
		}},
	},
}, {
	"sql":        "SELECT COUNT(DISTINCT user_id) as unique_users, COUNT(DISTINCT status) as unique_statuses FROM orders",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "orders",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":             nil,
			"unique_users":    bson.M{"$addToSet": "$user_id"},
			"unique_statuses": bson.M{"$addToSet": "$status"},
		}},
		{mongoProject: bson.M{
			"unique_users":    bson.M{"$size": "$unique_users"},
			"unique_statuses": bson.M{"$size": "$unique_statuses"},
		}},
	},
}, {
	"sql":        "SELECT DATE_FORMAT(created_at, '%Y-%m-%d') as date, COUNT(*) as count FROM orders GROUP BY DATE_FORMAT(created_at, '%Y-%m-%d')",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "orders",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id": bson.M{"$dateToString": bson.M{
				"format": "%Y-%m-%d",
				"date":   "$created_at",
			}},
			"count": bson.M{"$sum": 1},
		}},
		{mongoProject: bson.M{
			"date":  "$_id",
			"count": 1,
			"_id":   0,
		}},
	},
}, {
	"sql":        "SELECT CONCAT(first_name, ' ', last_name) as full_name, UPPER(email) as email_upper FROM users WHERE LOWER(status) = 'active'",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "users",
	"pipeline": []bson.M{
		{mongoMatch: bson.M{
			"status": bson.M{"$regex": "^active$", "$options": "i"},
		}},
		{mongoProject: bson.M{
			"full_name":   bson.M{"$concat": []interface{}{"$first_name", " ", "$last_name"}},
			"email_upper": bson.M{"$toUpper": "$email"},
		}},
	},
}, {
	"sql":        "SELECT category, AVG(price) as avg_price, (SELECT AVG(price) FROM products) as overall_avg FROM products GROUP BY category",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":       "$category",
			"avg_price": bson.M{"$avg": "$price"},
		}},
		{mongoLookup: bson.M{
			"from":     "products",
			"pipeline": []bson.M{{mongoGroup: bson.M{"_id": nil, "overall_avg": bson.M{"$avg": "$price"}}}},
			"as":       "overall",
		}},
		{"$unwind": "$overall"},
		{mongoProject: bson.M{
			"category":    "$_id",
			"avg_price":   1,
			"overall_avg": "$overall.overall_avg",
			"_id":         0,
		}},
	},
}, {
	"sql":        "SELECT name, price, AVG(price) OVER (PARTITION BY category) as category_avg FROM products",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id":          "$category",
			"category_avg": bson.M{"$avg": "$price"},
			"products": bson.M{"$push": bson.M{
				"name":  "$name",
				"price": "$price",
			}},
		}},
		{"$unwind": "$products"},
		{mongoProject: bson.M{
			"name":         "$products.name",
			"price":        "$products.price",
			"category_avg": 1,
		}},
	},
}, {
	"sql":        "SELECT u.name, d.name as dept_name FROM users u JOIN departments d ON u.dept_id = d.id",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "users",
	"pipeline": []bson.M{
		{mongoLookup: bson.M{
			"from":         "departments",
			"localField":   "dept_id",
			"foreignField": "id",
			"as":           "department",
		}},
		{"$unwind": "$department"},
		{mongoProject: bson.M{
			"name":      "$name",
			"dept_name": "$department.name",
		}},
	},
}, {
	"sql":        "SELECT u.name, d.name as dept_name FROM users u LEFT JOIN departments d ON u.dept_id = d.id",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "users",
	"pipeline": []bson.M{
		{mongoLookup: bson.M{
			"from":         "departments",
			"localField":   "dept_id",
			"foreignField": "id",
			"as":           "department",
		}},
		{"$unwind": bson.M{
			"path":                       "$department",
			"preserveNullAndEmptyArrays": true,
		}},
		{mongoProject: bson.M{
			"name":       "$name",
			"dept_name":  "$department.name",
			"department": "$department",
		}},
	},
}, {
	"sql":        "SELECT name, COALESCE(description, 'No description available') as description_text, IFNULL(price, 0) as final_price FROM products",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoProject: bson.M{
			"name": 1,
			"description_text": bson.M{
				"$ifNull": []interface{}{"$description", "No description available"},
			},
			"final_price": bson.M{
				"$ifNull": []interface{}{"$price", 0},
			},
		}},
	},
}, {
	"sql":        "SELECT SUBSTRING(name, 1, 3) as name_prefix, LENGTH(description) as desc_length FROM products",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoProject: bson.M{
			"name_prefix": bson.M{"$substr": []interface{}{"$name", 0, 3}},
			"desc_length": bson.M{"$strLenCP": "$description"},
		}},
	},
}, {
	"sql":        "SELECT * FROM products WHERE name REGEXP 'phone|smartphone' OR description REGEXP 'mobile|wireless'",
	"error":      nil,
	"operation":  "find",
	"collection": "products",
	"filter": bson.D{
		{Key: "$or", Value: []bson.M{
			{"name": bson.M{"$regex": "phone|smartphone"}},
			{"description": bson.M{"$regex": "mobile|wireless"}},
		}},
	},
}, {
	"sql":        "SELECT department, GROUP_CONCAT(DISTINCT name ORDER BY salary DESC) as employees FROM employees GROUP BY department",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "employees",
	"pipeline": []bson.M{
		{mongoGroup: bson.M{
			"_id": "$department",
			"names": bson.M{
				"$addToSet": "$name",
			},
		}},
		{"$sort": bson.M{"salary": -1}},
		{mongoProject: bson.M{
			"department": "$_id",
			"employees": bson.M{
				"$reduce": bson.M{
					"input":        "$names",
					"initialValue": "",
					"in": bson.M{
						"$concat": []interface{}{
							"$$value",
							bson.M{"$cond": []interface{}{
								bson.M{"$eq": []interface{}{"$$value", ""}},
								"",
								",",
							}},
							"$$this",
						},
					},
				},
			},
		}},
	},
}, {
	"sql":        "SELECT name, COALESCE(description, 'No description available') as desc, IFNULL(price, 0) as price FROM products",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoProject: bson.M{
			"name": 1,
			"desc": bson.M{
				"$ifNull": []interface{}{"$description", "No description available"},
			},
			"price": bson.M{
				"$ifNull": []interface{}{"$price", 0},
			},
		}},
	},
}, {
	"sql":        "SELECT SUBSTRING(name, 1, 3) as name_prefix, LENGTH(description) as desc_length, LOCATE('sale', LOWER(description)) as has_sale FROM products",
	"error":      nil,
	"operation":  "aggregate",
	"collection": "products",
	"pipeline": []bson.M{
		{mongoProject: bson.M{
			"name_prefix": bson.M{"$substr": []interface{}{"$name", 0, 3}},
			"desc_length": bson.M{"$strLenCP": "$description"},
			"has_sale": bson.M{
				"$indexOfCP": []interface{}{
					bson.M{"$toLower": "$description"},
					"sale",
				},
			},
		}},
	},
}}

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
