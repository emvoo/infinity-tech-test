package db

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"time"

	"infinity-tech-test/helpers"

	"github.com/go-sql-driver/mysql"
)

// StartDB sets-up DB instance and create database
// if doesn't exists yet
func StartDB() (*sql.DB, error) {
	config := mysql.NewConfig()
	host := helpers.GetEnvVariable("HOST")
	port := helpers.GetEnvVariable("PORT")
	config.Addr = net.JoinHostPort(host, port)
	config.User = helpers.GetEnvVariable("DB_USER")
	config.Passwd = helpers.GetEnvVariable("DB_PASS")
	config.ParseTime = true

	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(time.Duration(10) * time.Second)

	if err = db.Ping(); err != nil {
		return nil, err
	}

	stmt := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", helpers.GetEnvVariable("DB_NAME"))
	if _, err = db.Exec(stmt); err != nil {
		return nil, err
	}

	if _, err = db.Exec(fmt.Sprintf("USE %s", helpers.GetEnvVariable("DB_NAME"))); err != nil {
		return nil, err
	}

	return db, nil
}

// Migrate reads migrations directory and executes
// sql queries inside sql scripts
func Migrate(db *sql.DB) (err error) {
	root, err := os.Getwd()
	if err != nil {
		return
	}
	// best would be read whole directory but for this example
	// I suppose is enough to get migration file directly
	migrationFilePath := filepath.Join(root, "db/migrations/create_uploads_table.up.sql")
	file, err := os.Open(migrationFilePath)
	if err != nil {
		return
	}

	query, err := ioutil.ReadAll(file)
	if err != nil {
		return
	}

	if _, err = db.Exec(string(query)); err != nil {
		return
	}

	return
}
