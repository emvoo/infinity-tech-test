package models

import (
	"database/sql"
	"time"
)

// Upload represents database table
type Upload struct {
	ID                int64     `json:"id"`
	EventDateTime     time.Time `json:"event_date_time"`
	EventAction       string    `json:"event_action"`
	CallRef           int64     `json:"call_ref"`
	EventValue        float32   `json:"event_value,omitempty"`
	EventCurrencyCode string    `json:"event_currency_code,omitempty"`
}

// Insert attempts to insert data to database
// returns last inserted ID or error
func (u *Upload) Insert(db *sql.DB) (int64, error) {
	const stmt = "INSERT INTO uploads (`eventDatetime`, `eventAction`, `callRef`, `eventValue`, `eventCurrencyCode`) VALUES (?, ?, ?, ?, ?)"
	res, err := db.Exec(stmt, u.EventDateTime, u.EventAction, u.CallRef, u.EventValue, u.EventCurrencyCode)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}
