package helpers

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"log/syslog"
	"os"
	"path/filepath"
	"strings"
)

var (
	envMap map[string]string
	logger *syslog.Writer
)

func init() {
	var err error
	// initialize syslog logger
	logger, err = syslog.Dial("", "localhost", syslog.LOG_ERR, "infinity")
	if err != nil {
		log.Fatal(err)
	}

	// populate the map of env variables from .env file
	envMap, err = GetEnvVariables()
	if err != nil {
		logger.Err(fmt.Sprintf("error setting environment variables: %v", err))
		return
	}
}

// GetEnvVariable attempts to get the value of env variable by its name
func GetEnvVariable(name string) (envVar string) {
	envVar = envMap[name]
	if envVar == "" {
		log.Fatalf("environment variable %s seems to be empty", name)
	}
	return envVar
}

// GetEnvVariables populates the map of all existing
// env variables in .env file
// other way would be, while building the binary,
// to use '-ldflags' and set variables in this way
// but this way I think its easier to provide own values
func GetEnvVariables() (map[string]string, error) {
	root, err := os.Getwd()
	if err != nil {

		log.Printf("error getting project root directory: %v", err)
		return nil, err
	}

	var r io.Reader
	pathToEnvFile := filepath.Join(root, ".env")
	r, err = os.Open(pathToEnvFile)
	if err != nil {
		log.Fatalf("error reading file: %v", err)
	}

	var lines []string
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err = scanner.Err(); err != nil {
		return nil, err
	}

	envMap := map[string]string{}
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		keyValuePair := strings.Split(line, "=")
		if keyValuePair[0] == "" || len(keyValuePair) != 2 {
			return nil, fmt.Errorf("error getting environment variable")
		}

		key := keyValuePair[0]
		value := keyValuePair[1]

		envMap[key] = value
	}

	return envMap, nil
}
