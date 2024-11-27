package squeel

import (
	"fmt"
	"log"
	"os"
	"sync"
)

/*
Package-level variables for the logging system. The fileLogger is initialized once
using the sync.Once primitive to ensure thread-safe singleton initialization.
*/
var (
	fileLogger *log.Logger
	once       sync.Once
)

/*
getLogger returns a singleton logger instance that writes to both a file and stdout.
The logger is initialized only once using sync.Once to ensure thread safety.
It creates or opens a log file named "squeel.log" in the current working directory
with append mode and writes a test message to verify initialization.

Returns:
- A configured log.Logger instance that writes to both file and stdout
*/
func getLogger() *log.Logger {
	once.Do(func() {
		currentDir, _ := os.Getwd()
		fmt.Printf("Current working directory: %s\n", currentDir)

		file, err := os.OpenFile("squeel.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		fileLogger = log.New(file, "", log.LstdFlags|log.Lshortfile)

		// Log a test message to ensure the logger is working
		fileLogger.Println("Logger initialized")
	})
	return fileLogger
}

/*
logDebug writes a debug-level log message to both the log file and stdout.
It formats the message using the provided format string and arguments,
prefixing it with [DEBUG] for easy filtering.

Parameters:
- format: A format string following fmt.Printf conventions
- v: Variable arguments to be formatted according to the format string
*/
func logDebug(format string, v ...interface{}) {
	message := fmt.Sprintf(format, v...)
	getLogger().Printf("[DEBUG] %s", message)
	fmt.Printf("[DEBUG] %s\n", message) // Print to stdout as well
}

/*
LogDebug is the exported version of logDebug, currently disabled.
This function can be enabled to provide debug logging capabilities
to external packages using the squeel library.

Parameters:
- format: A format string following fmt.Printf conventions
- v: Variable arguments to be formatted according to the format string
*/
func LogDebug(format string, v ...interface{}) {
	// logDebug(format, v...)
}
