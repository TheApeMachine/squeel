package squeel

import (
	b64 "encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	errnie "github.com/theapemachine/errnie/v3"
	"github.com/xwb1989/sqlparser"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

/*
makeLegacyBinVal converts a base64 encoded string to a MongoDB Binary type

with subtype 3. It handles spaces in the input by replacing them with '+' characters
before decoding.
*/
func (statement *Statement) makeLegacyBinVal(value string) (
	binval primitive.Binary, err error,
) {
	var out []byte
	out, err = b64.StdEncoding.DecodeString(strings.ReplaceAll(value, " ", "+"))

	return primitive.Binary{
		Subtype: 3,
		Data:    out,
	}, errnie.Error(err)
}

/*
makeint64Pointer converts a SQL value to an int64 pointer. This is useful

when dealing with nullable integer columns in SQL that need to be represented
as pointers in Go.
*/
func (statement *Statement) makeint64Pointer(value *sqlparser.SQLVal) (*int64, error) {
	tmp, err := strconv.ParseInt(string(value.Val), 10, 64)
	return &tmp, errnie.Error(err)
}

/*
HexToBase64 converts a hexadecimal string to its base64 encoded representation.

If the hex decoding fails, it returns an empty byte slice and logs the error.
*/
func (statement *Statement) HexToBase64(in string) []byte {
	rawBytes, err := hex.DecodeString(in)

	if err != nil {
		fmt.Println("Error decoding hex:", errnie.Error(err).Error())
		return []byte{}
	}

	buf := make([]byte, b64.StdEncoding.EncodedLen(len(rawBytes)))
	b64.StdEncoding.Encode(buf, rawBytes)

	return buf
}

/*
CSUUID converts a UUID string to a MongoDB Binary type using a specific

reordering pattern. It removes any curly braces and hyphens from the UUID,
then reorders the bytes according to the required format before converting
to base64 and creating a Binary value.
*/
func (statement *Statement) CSUUID(uuid string) (primitive.Binary, error) {
	cleanUUID := strings.NewReplacer("{", "", "}", "", "-", "").Replace(uuid)

	reordered := cleanUUID[6:8] + cleanUUID[4:6] + cleanUUID[2:4] + cleanUUID[0:2] +
		cleanUUID[10:12] + cleanUUID[8:10] +
		cleanUUID[14:16] + cleanUUID[12:14] +
		cleanUUID[16:]

	return statement.makeLegacyBinVal(string(statement.HexToBase64(reordered)))
}

/*
makeLegacyBinVal is a package-level function that converts a base64 encoded

string to a MongoDB Binary type with subtype 3. It handles spaces in the input
by replacing them with '+' characters before decoding.
*/
func makeLegacyBinVal(value string) (
	binval primitive.Binary, err error,
) {
	var out []byte
	out, err = b64.StdEncoding.DecodeString(strings.ReplaceAll(value, " ", "+"))

	return primitive.Binary{
		Subtype: 3,
		Data:    out,
	}, errnie.Error(err)
}

/*
HexToBase64 is a package-level function that converts a hexadecimal string

to its base64 encoded representation. If the hex decoding fails, it returns
an empty byte slice and logs the error.
*/
func HexToBase64(in string) []byte {
	rawBytes, err := hex.DecodeString(in)

	if err != nil {
		fmt.Println("Error decoding hex:", errnie.Error(err).Error())
		return []byte{}
	}

	buf := make([]byte, b64.StdEncoding.EncodedLen(len(rawBytes)))
	b64.StdEncoding.Encode(buf, rawBytes)

	return buf
}

/*
CSUUID is a package-level function that converts a UUID string to a MongoDB

Binary type using a specific reordering pattern. It removes any curly braces
and hyphens from the UUID, then reorders the bytes according to the required
format before converting to base64 and creating a Binary value.
*/
func CSUUID(uuid string) (primitive.Binary, error) {
	cleanUUID := strings.NewReplacer("{", "", "}", "", "-", "").Replace(uuid)

	reordered := cleanUUID[6:8] + cleanUUID[4:6] + cleanUUID[2:4] + cleanUUID[0:2] +
		cleanUUID[10:12] + cleanUUID[8:10] +
		cleanUUID[14:16] + cleanUUID[12:14] +
		cleanUUID[16:]

	return makeLegacyBinVal(string(HexToBase64(reordered)))
}
