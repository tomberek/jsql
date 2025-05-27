package main

// FieldType represents a SQL field type
type FieldType string

const (
	TypeInt  FieldType = "INTEGER"
	TypeReal FieldType = "REAL"
	TypeText FieldType = "TEXT"
	TypeBool FieldType = "BOOLEAN"
	TypeJSON FieldType = "JSON"
)

// IndexDef represents an index definition
type IndexDef struct {
	Name    string   // Index name
	Table   string   // Table name
	Columns []string // Columns to index
	Unique  bool     // Whether the index is unique
}

// TableSchema represents the schema of a table
type TableSchema struct {
	Name    string
	Fields  map[string]FieldType
	FKs     map[string]string // column -> referenced table
	Indexes []IndexDef        // Indexes for this table
}

// DatabaseSchema represents the schema of the entire database
type DatabaseSchema struct {
	Tables     map[string]*TableSchema
	TableOrder []string
}

// stringSet is a utility type for tracking unique values
type stringSet map[string]struct{}
