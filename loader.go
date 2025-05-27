package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// InsertRow inserts a row into a table
// Shorter, always uses consistent marshaling for arrays/objects
func InsertRow(tx *sql.Tx, table *TableSchema, obj map[string]interface{}, dbs *DatabaseSchema) (int64, error) {
	cols := []string{}
	vals := []interface{}{}

	for field := range table.Fields {
		if field == "id" {
			continue
		}
		
		// Symbol table lookups
		if fk := table.FKs[field]; fk != "" && strings.HasSuffix(field, "_symbol") {
			val := obj[strings.TrimSuffix(field, "_symbol")]
			symTab := dbs.Tables[fk]
			id, err := getOrInsertSymbol(tx, symTab, val)
			if err != nil {
				return 0, err
			}
			cols = append(cols, field)
			vals = append(vals, id)
			continue
		}

		// Nested subtable
		if fk := table.FKs[field]; fk != "" && strings.HasSuffix(field, "_id") {
			base := strings.TrimSuffix(field, "_id")
			if v, ok := obj[base].(map[string]interface{}); ok && v != nil {
				subTab := dbs.Tables[fk]
				subID, err := InsertRow(tx, subTab, v, dbs)
				if err != nil {
					return 0, err
				}
				cols = append(cols, field)
				vals = append(vals, subID)
				continue
			}
			cols = append(cols, field)
			vals = append(vals, nil)
			continue
		}

		// Normal field
		raw, ok := obj[field]
		if !ok {
			cols = append(cols, field)
			vals = append(vals, nil)
			continue
		}
		switch raw.(type) {
		case []interface{}, map[string]interface{}:
			js, _ := json.Marshal(raw)
			cols = append(cols, field)
			vals = append(vals, string(js))
		default:
			cols = append(cols, field)
			vals = append(vals, raw)
		}
	}

	if len(cols) == 0 {
		return 0, nil
	}
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table.Name,
		strings.Join(cols, ", "),
		strings.TrimRight(strings.Repeat("?,", len(cols)), ","),
	)
	res, err := tx.Exec(q, vals...)
	if err != nil {
		return 0, fmt.Errorf("insert %s: %v (cols=%v vals=%v)", table.Name, err, cols, vals)
	}
	return res.LastInsertId()
}

// LoadData loads data from a JSON file into the database
func LoadData(jsonPath, dbPath string, dbs *DatabaseSchema) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	f, err := os.Open(jsonPath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	mainTable := dbs.Tables["main"]

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var obj map[string]interface{}
		if err := json.Unmarshal(line, &obj); err != nil {
			fmt.Fprintf(os.Stderr, "skip JSON line %d: %v\n", lineNum, err)
			continue
		}
		if _, err := InsertRow(tx, mainTable, obj, dbs); err != nil {
			fmt.Fprintf(os.Stderr, "Load row %d: %v\n", lineNum, err)
			continue
		}
	}
	return tx.Commit()
}