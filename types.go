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

// TableSchema represents the schema of a table
type TableSchema struct {
	Name   string
	Fields map[string]FieldType
	FKs    map[string]string // column -> referenced table
}

// DatabaseSchema represents the schema of the entire database
type DatabaseSchema struct {
	Tables     map[string]*TableSchema
	TableOrder []string
}

// stringSet is a utility type for tracking unique values
type stringSet map[string]struct{}