[![Tests](https://github.com/johndoe/awesome-project/actions/workflows/tests.yml/badge.svg)](https://github.com/johndoe/awesome-project/actions/workflows/tests.yml)

# Squeel 🔄

Squeel is a powerful SQL-to-MongoDB query translator that enables you to write MongoDB queries using familiar SQL syntax. It bridges the gap between SQL and MongoDB's query language, making it easier for developers familiar with SQL to work with MongoDB.

## ✨ Features

-   🔄 Translates SQL SELECT queries to MongoDB operations
-   🚀 Supports complex queries including:
    -   JOIN operations with `$lookup`
    -   Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
    -   WHERE clauses with multiple conditions
    -   GROUP BY and HAVING clauses
    -   ORDER BY for sorting
    -   LIMIT and OFFSET for pagination
-   🔧 Handles UUID fields automatically (converts to Binary for uppercase collections)
-   📦 Supports subqueries and nested field queries
-   ⚡ Maintains MongoDB's native performance characteristics

## 📥 Installation

```bash
go get github.com/fanfactory/data/squeel
```

## 🚀 Quick Start

```go
import "github.com/fanfactory/data/squeel"

// Initialize a new statement
sql := "SELECT name, age FROM users WHERE age > 21 ORDER BY name DESC LIMIT 10"
statement := squeel.NewStatement(sql)

// Build the query
query := squeel.NewQuery()
query, err := statement.Build(query)
if err != nil {
    log.Fatal(err)
}

// The resulting query object can be used with MongoDB driver
```

## 📖 Usage Examples

### Basic Queries

```sql
-- Simple SELECT
SELECT * FROM users

-- Select with UUID matching (automatically handles Binary conversion)
SELECT * FROM User WHERE _id = '695FF995-5DC4-4FBE-B80C-2621360D578F'

-- Select specific fields with conditions
SELECT first_name FROM user_profile WHERE _id = '695FF995-5DC4-4FBE-B80C-2621360D578F'

-- Pagination
SELECT * FROM fanchecks LIMIT 10 OFFSET 2
```

### Advanced Queries

```sql
-- JOIN with aggregation
SELECT u.name, p.city
FROM users u
JOIN profiles p ON u.id = p.user_id
WHERE u.age > 25

-- Complex aggregation with GROUP BY, HAVING, and ORDER BY
SELECT department, AVG(salary) as avg_salary
FROM employees
WHERE hire_date >= '2020-01-01'
GROUP BY department
HAVING AVG(salary) > 50000

-- Pattern matching and complex conditions
SELECT * FROM products
WHERE name LIKE '%phone%'
  AND (category = 'Electronics' OR category = 'Accessories')
  AND price BETWEEN 100 AND 500

-- Nested field queries
SELECT * FROM questions WHERE theme.nl = 'Some Theme'
```

## 🔧 Query Object Structure

Squeel translates SQL queries into a `Query` struct that can be used with the MongoDB driver:

```go
type Query struct {
    Context    context.Context
    Comment    string
    Operation  string        // "find", "findone", "aggregate", "count", "distinct"
    Collection string
    Filter     bson.D
    Projection bson.D
    Sort       bson.D
    Limit      *int64
    Offset     *int64
    Pipeline   mongo.Pipeline
    Payload    bson.D
}
```

### Query Operations

-   `find`: Regular SELECT queries
-   `findone`: SELECT with LIMIT 1
-   `aggregate`: Complex queries with JOIN, GROUP BY, or aggregation functions
-   `count`: COUNT queries
-   `distinct`: SELECT DISTINCT queries

### Automatic UUID Handling

Squeel automatically handles UUID fields differently based on collection naming:

-   Uppercase collections (e.g., `User`): UUIDs are converted to MongoDB Binary type
-   Lowercase collections (e.g., `users`): UUIDs remain as strings

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

Special thanks to all contributors who have helped make Squeel better!
