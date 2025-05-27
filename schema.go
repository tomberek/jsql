package main

import (
	"regexp"
	"sort"
	"strings"
)

// ParseDDL parses a DDL string and returns a DatabaseSchema
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

// resolveTableOrder determines the order in which tables should be created
// based on their dependencies
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
