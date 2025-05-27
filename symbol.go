package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

// getOrInsertSymbol retrieves or creates a symbol table entry
// Always marshals to JSON for consistency regardless of type
func getOrInsertSymbol(tx *sql.Tx, symTable *TableSchema, val interface{}) (int64, error) {
	if val == nil {
		return 0, nil
	}
	js, _ := json.Marshal(val)
	stored := string(js)

	var id int64
	err := tx.QueryRow(
		fmt.Sprintf("SELECT id FROM %s WHERE value = ?", symTable.Name),
		stored,
	).Scan(&id)
	if err == sql.ErrNoRows {
		_, err := tx.Exec(fmt.Sprintf("INSERT OR IGNORE INTO %s (value) VALUES (?)", symTable.Name), stored)
		if err != nil {
			return 0, err
		}
		err = tx.QueryRow(fmt.Sprintf("SELECT id FROM %s WHERE value = ?", symTable.Name), stored).Scan(&id)
		return id, err
	}
	return id, err
}

// getSymbolValue retrieves a symbol value by ID
func getSymbolValue(db *sql.DB, symTable string, id int64) (interface{}, error) {
	var val string
	err := db.QueryRow(
		fmt.Sprintf("SELECT value FROM %s WHERE id = ?", symTable), id,
	).Scan(&val)
	if err != nil {
		return nil, err
	}
	var v interface{}
	if err := json.Unmarshal([]byte(val), &v); err == nil {
		return v, nil
	}
	return val, nil
}