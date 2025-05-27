package main

import (
        "bytes"
	"bufio"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type FieldType string

const (
	TypeInt  FieldType = "INTEGER"
	TypeReal FieldType = "REAL"
	TypeText FieldType = "TEXT"
	TypeBool FieldType = "BOOLEAN"
	TypeJSON FieldType = "JSON"
)

// --- Table and Schema Structures ---
type TableSchema struct {
	Name   string
	Fields map[string]FieldType
	FKs    map[string]string // column -> referenced table
}

type DatabaseSchema struct {
	Tables     map[string]*TableSchema
	TableOrder []string
}

// --- DDL Parser ---
func ParseDDL(ddl string) *DatabaseSchema {
	lines := strings.Split(ddl, "\n")
	ds := &DatabaseSchema{Tables: map[string]*TableSchema{}}
	reCreate := regexp.MustCompile(`(?i)^CREATE TABLE (\w+)`)
	reField := regexp.MustCompile(`^\s*(\w+)\s+(\w+)(.*)$`)
	var curr *TableSchema
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		if m := reCreate.FindStringSubmatch(line); m != nil {
			curr = &TableSchema{
				Name:   m[1],
				Fields: map[string]FieldType{},
				FKs:    map[string]string{},
			}
			ds.Tables[m[1]] = curr
			continue
		}
		if curr == nil {
			continue
		}
		if strings.HasPrefix(line, ");") || line == ");" || line == ")" {
			curr = nil
			continue
		}
		if m := reField.FindStringSubmatch(line); m != nil {
			col, typ, rest := m[1], strings.ToUpper(m[2]), m[3]
			curr.Fields[col] = FieldType(typ)
			if strings.Contains(rest, "REFERENCES") {
				reFk := regexp.MustCompile(`REFERENCES\s+(\w+)`)
				mt := reFk.FindStringSubmatch(rest)
				if mt != nil {
					curr.FKs[col] = mt[1]
				}
			}
		}
	}
	ds.TableOrder = resolveTableOrder(ds.Tables)
	return ds
}

func resolveTableOrder(tables map[string]*TableSchema) []string {
	visited := map[string]bool{}
	var order []string
	var visit func(table string)
	visit = func(tbl string) {
		if visited[tbl] {
			return
		}
		for _, fk := range tables[tbl].FKs {
			visit(fk)
		}
		visited[tbl] = true
		order = append(order, tbl)
	}
	keys := make([]string, 0, len(tables))
	for k := range tables {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		visit(k)
	}
	return order
}

// --- DDL CREATE ---
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

// --- Symbol Table Helper ---
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

// --- Loader Core ---
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

func DumpRows(dbPath string, dbs *DatabaseSchema) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	main := dbs.Tables["main"]
	return dumpTable(db, dbs, main, "", nil)
}

type stringSet map[string]struct{}

// --- Schema Analyzer (infers DDL from LD-JSON) ---
func AnalyzeJSON(path string, sample int) string {
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "analyze: open:", err)
		os.Exit(1)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	var roots []map[string]interface{}
	for n := 0; n < sample && sc.Scan(); n++ {
		var rec map[string]interface{}
		if json.Unmarshal(sc.Bytes(), &rec) == nil {
			roots = append(roots, rec)
		}
	}
	if len(roots) == 0 {
		fmt.Fprintln(os.Stderr, "No rows for analysis")
		os.Exit(2)
	}

	// key: fieldname, val: set of unique string values
	fieldStringUniques := make(map[string]stringSet) // string fields
	fieldJSONUniques := make(map[string]stringSet)   // array/object fields

	schema := make(map[string]*TableSchema)
	analyzeObjectSymbol("main", roots, schema, fieldStringUniques, fieldJSONUniques)

	numRows := len(roots)
	symbolFields := map[string]bool{}
	symbolJSONFields := map[string]bool{}
	for field, uniques := range fieldStringUniques {
		if len(uniques) < numRows/5 {
			symbolFields[field] = true
		}
	}
	for field, uniques := range fieldJSONUniques {
		if len(uniques) < numRows/5 {
			symbolJSONFields[field] = true
		}
	}

	// Output DDL
	var sb strings.Builder
	order := resolveTableOrder(schema)
	for _, tbl := range order {
		ts := schema[tbl]
		sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", ts.Name))
		keys := make([]string, 0, len(ts.Fields))
		for k := range ts.Fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for j, k := range keys {
			switch {
			case symbolFields[k]:
				sb.WriteString(fmt.Sprintf("  %s_symbol INTEGER REFERENCES %s_symbol(id)", k, k))
			case symbolJSONFields[k]:
				sb.WriteString(fmt.Sprintf("  %s_symbol INTEGER REFERENCES %s_symbol(id)", k, k))
			default:
				sb.WriteString("  " + k + " " + string(ts.Fields[k]))
				if k == "id" {
					sb.WriteString(" PRIMARY KEY")
				}
				if fk, ok := ts.FKs[k]; ok {
					sb.WriteString(" REFERENCES " + fk + "(id)")
				}
			}
			if j < len(keys)-1 {
				sb.WriteString(",\n")
			}
		}
		sb.WriteString("\n);\n\n")
	}
	// Emit symbol table DDLs for string and JSON fields
	for field := range symbolFields {
		sb.WriteString(fmt.Sprintf("CREATE TABLE %s_symbol (\n  id INTEGER PRIMARY KEY,\n  value TEXT UNIQUE\n);\n\n", field))
	}
	for field := range symbolJSONFields {
		if _, already := symbolFields[field]; already {
			continue // already output
		}
		sb.WriteString(fmt.Sprintf("CREATE TABLE %s_symbol (\n  id INTEGER PRIMARY KEY,\n  value TEXT UNIQUE\n);\n\n", field))
	}
	return sb.String()
}

func analyzeObjectSymbol(
	tblName string,
	rows []map[string]interface{},
	schema map[string]*TableSchema,
	stringUniques map[string]stringSet,
	jsonUniques map[string]stringSet,
) {
	if _, ok := schema[tblName]; !ok {
		schema[tblName] = &TableSchema{Name: tblName, Fields: map[string]FieldType{}, FKs: map[string]string{}}
	}
	curr := schema[tblName]
	fieldTypes := map[string]FieldType{}

	for _, row := range rows {
		for k, v := range row {
			switch v2 := v.(type) {
			case map[string]interface{}:
				fieldTypes[k+"_id"] = TypeInt
				var subrows []map[string]interface{}
				for _, xrow := range rows {
					if sub, ok := xrow[k].(map[string]interface{}); ok {
						subrows = append(subrows, sub)
					}
				}
				analyzeObjectSymbol(k, subrows, schema, stringUniques, jsonUniques)
				curr.FKs[k+"_id"] = k
			case []interface{}:
				fieldTypes[k] = TypeJSON
				// Heuristic for symbolization: unique JSON-encoded values
				js, _ := json.Marshal(v2)
				if _, ok := jsonUniques[k]; !ok {
					jsonUniques[k] = stringSet{}
				}
				jsonUniques[k][string(js)] = struct{}{}
			case string:
				fieldTypes[k] = TypeText
				if _, ok := stringUniques[k]; !ok {
					stringUniques[k] = stringSet{}
				}
				stringUniques[k][v2] = struct{}{}
			case float64:
				fieldTypes[k] = TypeReal
			case bool:
				fieldTypes[k] = TypeBool
			default:
				fieldTypes[k] = TypeText
			}
		}
	}
	for f, t := range fieldTypes {
		curr.Fields[f] = t
	}
	curr.Fields["id"] = TypeInt
}

// --- CLI Entrypoint ---
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, `Usage:
  %s analyze --input data.json [--sample N]
  %s create-db --schema ddl.sql --db my.db
  %s load --input data.json --db my.db --schema ddl.sql
  %s dump --db my.db --schema ddl.sql
  %s import --input data.json --db my.db [--schema ddl.sql]
`, os.Args[0], os.Args[0], os.Args[0], os.Args[0],os.Args[0])
		os.Exit(1)
	}
	switch os.Args[1] {
	case "analyze":
		analyzeCmd(os.Args[2:])
	case "create-db":
		createDbCmd(os.Args[2:])
	case "load":
		loadCmd(os.Args[2:])
	case "dump":
		dumpCmd(os.Args[2:])
	case "import":
		importCmd(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}

func analyzeCmd(args []string) {
	flags := flag.NewFlagSet("analyze", flag.ExitOnError)
	var input string
	var sample int
	flags.StringVar(&input, "input", "", "Line-delimited JSON input file")
	flags.IntVar(&sample, "sample", 20, "How many rows to sample for schema inference")
	flags.Parse(args)
	if input == "" {
		fmt.Fprintf(os.Stderr, "--input is required\n")
		os.Exit(1)
	}
	fmt.Print(AnalyzeJSON(input, sample))
}

func createDbCmd(args []string) {
	flags := flag.NewFlagSet("create-db", flag.ExitOnError)
	var ddlFile, dbFile string
	flags.StringVar(&ddlFile, "schema", "", "SQL DDL file")
	flags.StringVar(&dbFile, "db", "", "SQLite database file")
	flags.Parse(args)
	if ddlFile == "" || dbFile == "" {
		fmt.Fprintln(os.Stderr, "--schema and --db are required")
		os.Exit(1)
	}
	ddl, err := os.ReadFile(ddlFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Read DDL:", err)
		os.Exit(1)
	}
	err = CreateDatabase(dbFile, string(ddl))
	if err != nil {
		fmt.Fprintln(os.Stderr, "Create DB:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "Wrote DB %s\n", dbFile)
}

func loadCmd(args []string) {
	flags := flag.NewFlagSet("load", flag.ExitOnError)
	var input, dbFile, ddlFile string
	flags.StringVar(&input, "input", "", "Line-delimited JSON input")
	flags.StringVar(&dbFile, "db", "", "SQLite database file")
	flags.StringVar(&ddlFile, "schema", "", "SQL DDL file (matching DB schema!)")
	flags.Parse(args)
	if input == "" || dbFile == "" || ddlFile == "" {
		fmt.Fprintln(os.Stderr, "--input, --db, and --schema are required")
		os.Exit(1)
	}
	ddl, err := os.ReadFile(ddlFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Read DDL:", err)
		os.Exit(1)
	}
	dbSchema := ParseDDL(string(ddl))
	err = LoadData(input, dbFile, dbSchema)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Data load error:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "Loaded %s into %s\n", input, dbFile)
}

func dumpCmd(args []string) {
	flags := flag.NewFlagSet("dump", flag.ExitOnError)
	var dbFile, ddlFile string
	flags.StringVar(&dbFile, "db", "", "SQLite database file")
	flags.StringVar(&ddlFile, "schema", "", "SQL DDL file")
	flags.Parse(args)
	if dbFile == "" || ddlFile == "" {
		fmt.Fprintln(os.Stderr, "--db and --schema are required")
		os.Exit(1)
	}
	ddl, err := os.ReadFile(ddlFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Read DDL:", err)
		os.Exit(1)
	}
	dbSchema := ParseDDL(string(ddl))
	err = DumpRows(dbFile, dbSchema)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Dump error:", err)
		os.Exit(1)
	}
}

func importCmd(args []string) {
	flags := flag.NewFlagSet("import", flag.ExitOnError)
	var input, dbFile, ddlFile string
	var sample int
	flags.StringVar(&input, "input", "", "Line-delimited JSON input")
	flags.StringVar(&dbFile, "db", "", "SQLite database output")
	flags.StringVar(&ddlFile, "schema", "", "If supplied, write DDL to this file")
	flags.IntVar(&sample, "sample", 20, "How many rows to sample for schema inference")
	flags.Parse(args)
	if input == "" || dbFile == "" {
		fmt.Fprintln(os.Stderr, "--input and --db required")
		os.Exit(1)
	}
	ddl := AnalyzeJSON(input, sample)
	if ddlFile != "" {
		if err := os.WriteFile(ddlFile, []byte(ddl), 0666); err != nil {
			fmt.Fprintln(os.Stderr, "Write DDL:", err)
			os.Exit(1)
		}
	}
	if err := CreateDatabase(dbFile, ddl); err != nil {
		fmt.Fprintln(os.Stderr, "Create DB:", err)
		os.Exit(1)
	}
	dbSchema := ParseDDL(ddl)
	if err := LoadData(input, dbFile, dbSchema); err != nil {
		fmt.Fprintln(os.Stderr, "Load data:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "Imported %s to %s\n", input, dbFile)
}
