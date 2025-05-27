package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// AnalyzeJSON analyzes a JSON file and returns a SQL DDL string
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

// analyzeObjectSymbol analyzes an object and its fields to determine the schema
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