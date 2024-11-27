package squeel

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	errnie "github.com/theapemachine/errnie/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

/*
Package-level constants defining field names used in queries.
*/
const (
	userAccountsField = "User.Accounts"
)

/*
handleVisibleAt processes a visibility time filter, creating a time window
around the specified time. The window extends 15 minutes before the given time.

Parameters:
- values: The time value to process in various supported formats

Returns:
- Any error that occurred during time parsing or filter creation
*/
func (query *Query) handleVisibleAt(values string) error {
	t, err := parseTimeWithFormats(values)
	if err != nil {
		return err
	}
	windowStart := t.Add(-15 * time.Minute)
	query.Filter = append(query.Filter, bson.E{Key: "VisibleAt", Value: bson.M{
		"$lte": t,
		"$gte": windowStart,
	}})
	return nil
}

/*
handleBirthDay processes a birthday filter, extracting day and month from
the input and creating a MongoDB expression to match these components.

Parameters:
- values: The birthday value in DD-MM format

Returns:
- Any error that occurred during parsing or filter creation
*/
func (query *Query) handleBirthDay(values string) error {
	day, month, err := parseBirthDayFormat(values)
	if err != nil {
		return err
	}
	query.Filter = append(query.Filter, bson.E{Key: "$expr", Value: bson.M{
		"$and": []bson.M{
			{"$eq": []interface{}{bson.M{"$month": "$BirthDay"}, month}},
			{"$eq": []interface{}{bson.M{"$dayOfMonth": "$BirthDay"}, day}},
		},
	}})
	return nil
}

/*
handleAccountId processes an account ID filter, setting up an aggregation
pipeline to join with the User and Account collections.

Parameters:
- values: The account ID value to process

Returns:
- Any error that occurred during UUID parsing or pipeline setup
*/
func (query *Query) handleAccountId(values string) error {
	uid, err := CSUUID(values)
	if err != nil {
		return err
	}
	query.Operation = "aggregate"
	query.Collection = "Device"
	query.Pipeline = buildAccountPipeline(uid)
	return nil
}

/*
handleLeaveDate processes a leave date filter, creating a date range that
spans the entire specified day.

Parameters:
- values: The leave date in YYYY-MM-DD format

Returns:
- Any error that occurred during date parsing or filter creation
*/
func (query *Query) handleLeaveDate(values string) error {
	startDate, err := time.Parse("2006-01-02", values)
	if err != nil {
		return fmt.Errorf("invalid date format for LeaveDate: %s, expected YYYY-MM-DD", values)
	}
	endDate := startDate.Add(24 * time.Hour)
	query.Filter = append(query.Filter, bson.E{Key: "LeaveDate", Value: bson.M{
		"$gte": startDate,
		"$lt":  endDate,
	}})
	return nil
}

/*
buildAccountPipeline creates a MongoDB aggregation pipeline for account-related
queries. It sets up stages to join with User and Account collections and applies
various filters including module access checks.

Parameters:
- uid: The account UUID to filter by

Returns:
- A MongoDB pipeline configured for account-related queries
*/
func buildAccountPipeline(uid primitive.Binary) mongo.Pipeline {
	return mongo.Pipeline{
		bson.D{{Key: "$lookup", Value: bson.M{
			"from": "User", "localField": "UserId",
			"foreignField": "_id", "as": "User",
		}}},
		bson.D{{Key: "$unwind", Value: "$User"}},
		bson.D{{Key: "$lookup", Value: bson.M{
			"from": "Account", "localField": userAccountsField,
			"foreignField": "_id", "as": "AccountDetails",
		}}},
		bson.D{{Key: "$match", Value: bson.M{
			userAccountsField: bson.M{"$in": []primitive.Binary{uid}},
			"User.Deleted":    nil,
			"AccountDetails": bson.M{
				"$elemMatch": bson.M{
					"_id":     uid,
					"Modules": bson.M{"$in": []int{14}},
				},
			},
		}}},
		bson.D{{Key: "$project", Value: bson.M{
			"_id": 1, "PushToken": 1, userAccountsField: 1,
		}}},
	}
}

/*
parseTimeWithFormats attempts to parse a time string using multiple supported
formats. It tries each format in sequence until one succeeds.

Parameters:
- value: The time string to parse

Returns:
- The parsed time.Time value
- Any error that occurred during parsing
*/
func parseTimeWithFormats(value string) (time.Time, error) {
	formats := []string{
		time.RFC3339,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05",
	}
	for _, format := range formats {
		if t, err := time.Parse(format, value); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unable to parse time: %s", value)
}

/*
parseBirthDayFormat parses a birthday string in DD-MM format into separate
day and month components.

Parameters:
- value: The birthday string to parse

Returns:
- The day component
- The month component
- Any error that occurred during parsing
*/
func parseBirthDayFormat(value string) (day, month int, err error) {
	parts := strings.Split(value, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid birthday format: %s, expected DD-MM", value)
	}
	day, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid day format: %s", parts[0])
	}
	month, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid month format: %s", parts[1])
	}
	return day, month, nil
}

/*
ParseRequest processes an HTTP request's query parameters and updates the Query
object accordingly. It handles both SQL queries and specific field filters.

Parameters:
- ctx: The Fiber context containing the request information

Returns:
- The modified Query object, or nil if an error occurred
*/
func (query *Query) ParseRequest(ctx fiber.Ctx) *Query {
	for key, values := range ctx.Queries() {
		if key == "sql" {
			return handleSQLQuery(values, query)
		}

		var err error
		switch key {
		case "VisibleAt":
			err = query.handleVisibleAt(values)
		case "BirthDay":
			err = query.handleBirthDay(values)
		case "AccountId":
			err = query.handleAccountId(values)
		case "groups":
			err = query.handleGroups(values)
		case "LeaveDate":
			err = query.handleLeaveDate(values)
		default:
			query.Filter = append(query.Filter, bson.E{Key: key, Value: values})
		}

		if err != nil {
			errnie.Error(err)
			return nil
		}
	}

	if query.Operation == "" {
		query.Operation = ctx.Params("operation")
	}
	if query.Collection == "" {
		query.Collection = ctx.Params("collection")
	}

	return query
}

/*
handleSQLQuery processes a SQL query string, parsing it and building the
appropriate MongoDB query configuration.

Parameters:
- values: The SQL query string
- query: The Query object to modify

Returns:
- The modified Query object, or nil if an error occurred
*/
func handleSQLQuery(values string, query *Query) *Query {
	stmt := NewStatement(values)
	var err error
	if query, err = stmt.Build(query); errnie.Error(err) != nil {
		return nil
	}
	return query
}

/*
handleGroups processes a groups filter, adding a condition to match documents
where the specified group ID is in the Groups array.

Parameters:
- values: The group ID to filter by

Returns:
- Any error that occurred during UUID parsing or filter creation
*/
func (query *Query) handleGroups(values string) error {
	uid, err := CSUUID(values)
	if err != nil {
		return err
	}
	query.Filter = append(query.Filter, bson.E{
		Key:   "Groups._id",
		Value: bson.M{"$in": []primitive.Binary{uid}},
	})
	return nil
}
