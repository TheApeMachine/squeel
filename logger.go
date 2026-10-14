package squeel

import (
	"fmt"

	errnie "github.com/theapemachine/errnie/v3"
)

/*
LogDebug writes a debug-level log message using the errnie logging framework.

Parameters:
  - format: A format string following fmt.Printf conventions
  - v: Variable arguments to be formatted according to the format string
*/
func LogDebug(format string, v ...any) {
	if len(v) == 0 {
		errnie.Debug(format)
		return
	}
	errnie.Debug(fmt.Sprintf(format, v...))
}
