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

func (statement *Statement) makeint64Pointer(value *sqlparser.SQLVal) (*int64, error) {
	tmp, err := strconv.ParseInt(string(value.Val), 10, 64)
	return &tmp, errnie.Error(err)
}

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

func (statement *Statement) CSUUID(uuid string) (primitive.Binary, error) {
	cleanUUID := strings.NewReplacer("{", "", "}", "", "-", "").Replace(uuid)

	reordered := cleanUUID[6:8] + cleanUUID[4:6] + cleanUUID[2:4] + cleanUUID[0:2] +
		cleanUUID[10:12] + cleanUUID[8:10] +
		cleanUUID[14:16] + cleanUUID[12:14] +
		cleanUUID[16:]

	return statement.makeLegacyBinVal(string(statement.HexToBase64(reordered)))
}

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

func CSUUID(uuid string) (primitive.Binary, error) {
	cleanUUID := strings.NewReplacer("{", "", "}", "", "-", "").Replace(uuid)

	reordered := cleanUUID[6:8] + cleanUUID[4:6] + cleanUUID[2:4] + cleanUUID[0:2] +
		cleanUUID[10:12] + cleanUUID[8:10] +
		cleanUUID[14:16] + cleanUUID[12:14] +
		cleanUUID[16:]

	return makeLegacyBinVal(string(HexToBase64(reordered)))
}
