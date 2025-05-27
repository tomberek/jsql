package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// determineIndexes generates index definitions based on heuristics and options
func determineIndexes(schema map[string]*TableSchema, symbolFields, symbolJSONFields map[string]bool, opts AnalyzeOptions) {
	if !opts.GenerateIndexes {
		return
	}

	// For each table in the schema
	for _, ts := range schema {
		// Track which fields we've already included in an index
		indexedFields := make(map[string]bool)

		// 1. Create indexes for foreign key fields
		if opts.IndexFKs {
			for fkField, _ := range ts.FKs {
				// Skip id field as it's already a primary key
				if fkField == "id" {
					continue
				}

				// Create index for foreign key
				idx := IndexDef{
					Name:    fmt.Sprintf("idx_%s_%s", ts.Name, fkField),
					Table:   ts.Name,
					Columns: []string{fkField},
					Unique:  false,
				}
				ts.Indexes = append(ts.Indexes, idx)
				indexedFields[fkField] = true
			}
		}

		// 2. Create indexes for symbol fields (these are high-cardinality fields that were converted to symbols)
		if opts.IndexSymbols {
			for field, _ := range ts.Fields {
				symbolField := field + "_symbol"
				if _, ok := ts.Fields[symbolField]; ok {
					if !indexedFields[symbolField] {
						idx := IndexDef{
							Name:    fmt.Sprintf("idx_%s_%s", ts.Name, symbolField),
							Table:   ts.Name,
							Columns: []string{symbolField},
							Unique:  false,
						}
						ts.Indexes = append(ts.Indexes, idx)
						indexedFields[symbolField] = true
					}
				}
			}
		}
	}

	// Create indexes for symbol tables (always add these for efficient lookups)
	if opts.IndexSymbols {
		for field := range symbolFields {
			if ts, ok := schema[field+"_symbol"]; ok {
				// Symbol tables already have a unique index on 'value' due to UNIQUE constraint
				// But we'll add an explicit index definition for clarity
				idx := IndexDef{
					Name:    fmt.Sprintf("idx_%s_value", field+"_symbol"),
					Table:   field + "_symbol",
					Columns: []string{"value"},
					Unique:  true,
				}
				ts.Indexes = append(ts.Indexes, idx)
			}
		}

		for field := range symbolJSONFields {
			if _, already := symbolFields[field]; already {
				continue // already output
			}
			if ts, ok := schema[field+"_symbol"]; ok {
				idx := IndexDef{
					Name:    fmt.Sprintf("idx_%s_value", field+"_symbol"),
					Table:   field + "_symbol",
					Columns: []string{"value"},
					Unique:  true,
				}
				ts.Indexes = append(ts.Indexes, idx)
			}
		}
	}
}

// AnalyzeOptions contains options for JSON analysis
type AnalyzeOptions struct {
	Sample          int  // Number of records to sample
	GenerateIndexes bool // Whether to generate indexes
	IndexFKs        bool // Whether to index foreign keys
	IndexSymbols    bool // Whether to index symbol fields
}

// DefaultAnalyzeOptions returns the default options for analysis
func DefaultAnalyzeOptions() AnalyzeOptions {
	return AnalyzeOptions{
		Sample:          20,
		GenerateIndexes: true,
		IndexFKs:        true,
		IndexSymbols:    true,
	}
}

// AnalyzeJSON analyzes a JSON file and returns a SQL DDL string
func AnalyzeJSON(path string, sample int) string {
	opts := DefaultAnalyzeOptions()
	opts.Sample = sample
	return AnalyzeJSONWithOptions(path, opts)
}

// AnalyzeJSONWithOptions analyzes a JSON file with custom options and returns a SQL DDL string
func AnalyzeJSONWithOptions(path string, opts AnalyzeOptions) string {
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "analyze: open:", err)
		os.Exit(1)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	var roots []map[string]interface{}
	for n := 0; n < opts.Sample && sc.Scan(); n++ {
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

	// Generate indexes based on heuristics and options
	determineIndexes(schema, symbolFields, symbolJSONFields, opts)

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

	// Emit CREATE INDEX statements
	for _, tbl := range order {
		ts := schema[tbl]
		for _, idx := range ts.Indexes {
			uniqueStr := ""
			if idx.Unique {
				uniqueStr = "UNIQUE "
			}
			columns := strings.Join(idx.Columns, ", ")
			sb.WriteString(fmt.Sprintf("CREATE %sINDEX %s ON %s (%s);\n",
				uniqueStr, idx.Name, idx.Table, columns))
		}
		if len(ts.Indexes) > 0 {
			sb.WriteString("\n")
		}
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
		schema[tblName] = &TableSchema{
			Name:    tblName,
			Fields:  map[string]FieldType{},
			FKs:     map[string]string{},
			Indexes: []IndexDef{},
		}
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
