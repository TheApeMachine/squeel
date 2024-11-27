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

const (
	userAccountsField = "User.Accounts"
)

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

func handleSQLQuery(values string, query *Query) *Query {
	stmt := NewStatement(values)
	var err error
	if query, err = stmt.Build(query); errnie.Error(err) != nil {
		return nil
	}
	return query
}

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
