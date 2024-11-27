package squeel

import (
	"fmt"
	"log"
	"os"
	"sync"
)

var (
	fileLogger *log.Logger
	once       sync.Once
)

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

func logDebug(format string, v ...interface{}) {
	message := fmt.Sprintf(format, v...)
	getLogger().Printf("[DEBUG] %s", message)
	fmt.Printf("[DEBUG] %s\n", message) // Print to stdout as well
}

func LogDebug(format string, v ...interface{}) {
	// logDebug(format, v...)
}
