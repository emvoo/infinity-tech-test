package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"log/syslog"
	"os"
	"path/filepath"
	"strconv"
	"time"
	"unicode/utf8"

	"infinity-tech-test/db"
	"infinity-tech-test/db/models"

	"golang.org/x/sys/unix"
	"golang.org/x/text/currency"
)

const layout = "2006-01-02 15:04:05"

var (
	logger *syslog.Writer
	root   string
)

// Record struct represents each csv file
// containing slice of headers as well as all rows
type Record struct {
	Headers []string
	Values  [][]string
}

func main() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	var err error
	// initialize project root directory
	root, err = os.Getwd()
	if err != nil {
		logger.Err(fmt.Sprintf("error getting project root path: %v", err))
		return
	}

	// create lock file so only one instance of the program will run at once
	file, err := createLockFile()
	if err != nil {
		log.Fatalf("unable to create lock file: %v", err)
	}

	// unlock the file after all other operations of this program finish
	defer unlockLockFile(file)

	// initialize syslog logger
	logger, err = syslog.Dial("", "localhost", syslog.LOG_ERR, "infinity")
	if err != nil {
		log.Fatal(err)
	}

	// initialize database
	DB, err := db.StartDB()
	if err != nil {
		logger.Err(fmt.Sprintf("error straing db: %v", err))
		return
	}

	// run migration(s) to create table(s)
	if err = db.Migrate(DB); err != nil {
		logger.Err(fmt.Sprintf("error creating infinity table: %v", err))
		return
	}

	// handle files uploaded by the client
	uploads, err := HandleUploads()
	if err != nil {
		logger.Err(fmt.Sprintf("something went terribly wrong: %v", err))
		return
	}

	uploadModels, err := csvToUploadModel(uploads)
	if err != nil {
		logger.Err(fmt.Sprintf("error converting csv files to upload models: %v", err))
		return
	}

	validateAndInsert(uploadModels, DB)

	if err = moveToProcessed(uploads); err != nil {
		logger.Err(fmt.Sprintf("error moving files: %v", err))
		return
	}
}

// HandleUploads reads contents of 'uploaded' folder
func HandleUploads() (uploads []os.FileInfo, err error) {
	path := filepath.Join(root, "uploaded")
	uploads, err = ioutil.ReadDir(path)
	if err != nil {
		logger.Err(fmt.Sprintf("error reading folder contents: %v", err))
		return
	}
	return
}

// csvToUploadModel reads each csv file,
// manipulates data to create models.Upload object
func csvToUploadModel(uploads []os.FileInfo) (uploadModels []models.Upload, err error) {
	var contents [][]string
	path := filepath.Join(root, "uploaded") // create uploaded directory
	// loop over directory contents and read each csv file
	for _, uploadedFile := range uploads {
		file, err := os.Open(filepath.Join(path, uploadedFile.Name()))
		if err != nil {
			logger.Err(fmt.Sprintf("error opening file: %v", err))
			return uploadModels, err
		}
		// create csv reader
		reader := csv.NewReader(file)
		fileContents, err := reader.ReadAll()
		// handle unsuccessful attempt to read data
		if err != nil {
			logger.Err(fmt.Sprintf("error getting file contents: %v", err))
			return uploadModels, err
		}
		// make sure file contains 5 columns
		// otherwise is being considered non standard and/or broken
		// and should not be handled/added to the queue
		for _, record := range fileContents {
			if len(record) != 5 {
				logger.Warning(fmt.Sprintf("file %s contains unexpected number of columns and has been skiiped for further processing", uploadedFile.Name()))
				break
			}

			contents = append(contents, record)
		}
	}

	// convert raw csv data to structured data
	var records []Record
	for _, content := range contents {
		if isHeader(content) {
			rec := new(Record)
			rec.Headers = content
			records = append(records, *rec)
		} else {
			i := len(records) - 1
			rec := records[i]
			rec.Values = append(rec.Values, content)
			records[i] = rec
		}
	}

	// convert structured data to []models.Upload
	uploadModels = make([]models.Upload, len(records))
	errors := []error{}
	for i, record := range records {
		upload := new(models.Upload)
		for j, header := range record.Headers {
			for _, values := range record.Values {
				switch header {
				case "eventDatetime":
					eventDateToime, err := time.Parse(layout, values[j])
					if err != nil {
						errors = append(errors, err)
						logger.Err(fmt.Sprintf("error parsing date (%s): %v", values[j], err))
					}
					upload.EventDateTime = eventDateToime
					break
				case "eventAction":
					upload.EventAction = values[j]
					break
				case "callRef":
					callRef, err := strconv.ParseInt(values[j], 10, 64)
					if err != nil {
						errors = append(errors, err)
						logger.Err(fmt.Sprintf("error converting string (%s) to int64: %v", values[j], err))
					}
					upload.CallRef = callRef
					break
				case "eventValue":
					eventValue, err := strconv.ParseFloat(values[j], 32)
					if err != nil {
						errors = append(errors, err)
						logger.Err(fmt.Sprintf("error converting string (%s) to float32: %v", values[j], err))
					}
					upload.EventValue = float32(eventValue)
					break
				case "eventCurrencyCode":
					upload.EventCurrencyCode = values[j]
					break
				default:
					logger.Info("this error should never happen")
					return uploadModels, fmt.Errorf("this should not happen, yet it did")
				}
			}

		}
		// initial validation
		// more should be done towards logging which file
		// failed validation and should possibly be moved to
		// other than 'processed' directory
		if len(errors) > 0 {
			logger.Err(fmt.Sprintf("validation errors: %v", errors))
		}
		uploadModels[i] = *upload
	}
	return
}

// validateAndInsert validates each field of the model
// and attempts to insert only those models that pass validation
func validateAndInsert(uploadModels []models.Upload, db *sql.DB) {
	for _, model := range uploadModels {
		if ok, err := validate(model); !ok {
			logger.Err(fmt.Sprintf("validation failed: %v", err))
			continue
		}

		_, err := model.Insert(db)
		if err != nil {
			// log the error but do not stop the execution, attempt to insert all data
			logger.Err(fmt.Sprintf("sql error inserting new data: %v", err))
			continue
		}
	}
}

// moveToProcessed handles moving files to 'processed directory'
func moveToProcessed(uploads []os.FileInfo) error {
	path := filepath.Join(root, "uploaded")
	for _, file := range uploads {
		oldPath := filepath.Join(path, file.Name())
		newPath := filepath.Join(root, "processed", file.Name())
		if err := os.Rename(oldPath, newPath); err != nil {
			logger.Err(fmt.Sprintf("error moving file %s", file.Name()))
		}
	}

	return nil
}

// isHeader checks if csv file contains valid columns
func isHeader(records []string) bool {
	allowedColumnNames := []string{"eventDatetime", "eventAction", "callRef", "eventValue", "eventCurrencyCode"}
	allowedColumnNamesMap := make(map[string]struct{}, len(allowedColumnNames))
	for _, name := range allowedColumnNames {
		allowedColumnNamesMap[name] = struct{}{}
	}

	for _, record := range records {
		if _, ok := allowedColumnNamesMap[record]; !ok {
			return false
		}
	}
	return true
}

// validate checks each value of the model against set criteria in the tech-test
func validate(model models.Upload) (bool, error) {
	// makes sure date is not zero value
	// no need to check against other possibilities
	// as if parsing fails defaults to zeroTime
	zeroTime := time.Time{}
	if model.EventDateTime == zeroTime {
		return false, fmt.Errorf("eventDateTime field is required, provided value: %s", model.EventDateTime)
	}

	// eventAction should be between 1-20 characters
	eventActionLength := utf8.RuneCountInString(model.EventAction)
	if eventActionLength < 1 || eventActionLength > 20 {
		return false, fmt.Errorf("eventAction field must have between 1-20 characters, %s given", eventActionLength)
	}

	// assume 0 is incorrect value
	if model.CallRef == 0 {
		return false, fmt.Errorf("callRef field is required, %d given", model.CallRef)
	}

	// get length of currency code
	currencyCodeLength := utf8.RuneCountInString(model.EventCurrencyCode)
	// if eventValue exists then currency code must exists and be 3 chars long
	if model.EventValue != 0 && currencyCodeLength != 3 {
		return false, fmt.Errorf("either eventValue is missing or currencyCode has incorrect length")
	}

	// makes sure currency code is valid ISO4217
	if _, err := currency.ParseISO(model.EventCurrencyCode); err != nil {
		return false, err
	}

	return true, nil
}

// createLockFile create file and locks it
func createLockFile() (*os.File, error) {
	file, err := os.Create("example.lock")
	if err != nil {
		return nil, err
	}

	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		return nil, err
	}

	return file, nil
}

// unlockLockFile unlocks the file
func unlockLockFile(file *os.File) error {
	if err := unix.Flock(int(file.Fd()), unix.LOCK_UN); err != nil {
		return err
	}

	return nil
}
