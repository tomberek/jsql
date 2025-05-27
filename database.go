package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// CreateDatabase creates a new SQLite database with the given schema
func CreateDatabase(dbPath string, ddl string) error {
	os.Remove(dbPath)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(ddl)
	return err
}

// DumpRows dumps all rows from the main table in the database
func DumpRows(dbPath string, dbs *DatabaseSchema) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	main := dbs.Tables["main"]
	return dumpTable(db, dbs, main, "", nil)
}

// dumpTable dumps all rows from a table in the database
func dumpTable(db *sql.DB, dbs *DatabaseSchema, table *TableSchema, whereClause string, args []any) error {
	query := fmt.Sprintf("SELECT * FROM %s", table.Name)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	for rows.Next() {
		valPtrs := make([]interface{}, len(columns))
		vals := make([]interface{}, len(columns))
		for i := range columns {
			valPtrs[i] = &vals[i]
		}
		if err := rows.Scan(valPtrs...); err != nil {
			return err
		}
		obj, err := dumpRowValueSet(db, dbs, table, columns, vals)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(obj)
	}
	return nil
}

// dumpRowByID dumps a single row from a table in the database
func dumpRowByID(db *sql.DB, dbs *DatabaseSchema, table *TableSchema, id int64) (map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE id = ?", table.Name)
	row := db.QueryRow(query, id)
	cols, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 1", table.Name))
	if err != nil {
		return nil, err
	}
	defer cols.Close()
	columns, _ := cols.Columns()

	vals := make([]interface{}, len(columns))
	valPtrs := make([]interface{}, len(columns))
	for i := range columns {
		valPtrs[i] = &vals[i]
	}

	err = row.Scan(valPtrs...)
	if err != nil {
		return nil, err
	}
	return dumpRowValueSet(db, dbs, table, columns, vals)
}

// dumpRowValueSet processes a row's values and returns a map representation
func dumpRowValueSet(db *sql.DB, dbs *DatabaseSchema, table *TableSchema, columns []string, vals []interface{}) (map[string]interface{}, error) {
	obj := map[string]interface{}{}
	fkFields := map[string]string{}
	symbolFields := map[string]string{}
	for col, ref := range table.FKs {
		if strings.HasSuffix(col, "_symbol") {
			symbolFields[col] = ref
		} else if strings.HasSuffix(col, "_id") {
			fkFields[col] = ref
		}
	}

	for i, col := range columns {
		if vals[i] == nil {
			continue
		}
		val := vals[i]

		if col == "id" {
			continue
		}
		// SYMBOL
		if symtable, isSym := symbolFields[col]; isSym {
			var symId int64
			switch vv := val.(type) {
			case int64:
				symId = vv
			case int:
				symId = int64(vv)
			case []byte:
				fmt.Sscanf(string(vv), "%d", &symId)
			}
			s, err := getSymbolValue(db, symtable, symId)
			if err == nil {
				obj[strings.TrimSuffix(col, "_symbol")] = s
			}
			continue
		}
		// SUB-TABLE FK
		if subtbl, isFK := fkFields[col]; isFK {
			var subid int64
			switch sv := val.(type) {
			case int64:
				subid = sv
			case int:
				subid = int64(sv)
			case []byte:
				fmt.Sscanf(string(sv), "%d", &subid)
			}
			if subid == 0 {
				// Do NOT assign anything if the field was NULL: faithfully omits the field.
				continue
			}
			subTable := dbs.Tables[subtbl]
			subObj, err := dumpRowByID(db, dbs, subTable, subid)
			if err == nil && subObj != nil && len(subObj) > 0 {
				obj[strings.TrimSuffix(col, "_id")] = subObj
			}
			// else: do not assign (omit). Faithfully omits if missing or could not resolve.
			continue
		}
		// JSON/TEXT columns that might be arrays/objects
		if table.Fields[col] == TypeJSON || table.Fields[col] == TypeText {
			switch vv := val.(type) {
			case []byte:
				text := string(vv)
				if len(text) > 0 && (text[0] == '[' || text[0] == '{') {
					var out interface{}
					if err := json.Unmarshal([]byte(text), &out); err == nil {
						obj[col] = out
						continue
					}
				}
				obj[col] = text
			case string:
				text := vv
				if len(text) > 0 && (text[0] == '[' || text[0] == '{') {
					var out interface{}
					if err := json.Unmarshal([]byte(text), &out); err == nil {
						obj[col] = out
						continue
					}
				}
				obj[col] = text
			default:
				obj[col] = val
			}
			continue
		}
		obj[col] = val
	}
	return obj, nil
}