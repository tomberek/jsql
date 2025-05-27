# JSON to SQL

## Quick start
```
go run ./... analyze --input some.json > schema
go run ./... create-db --db db --schema schema
go run ./... load --db db --schema schema --input some.json

# or to do it all in one go...
go run ./... import --db db --schema schema --input some.json

# dump
go run ./... dump --schema schema --db db
```

# JSQL Schema Guide

This document explains how to write and edit schemas for the JSQL tool to structure your data.

## Schema Basics

A schema in JSQL is SQLite DDL (Data Definition Language) that defines tables, columns, and relationships. While the tool can auto-generate schemas from JSON, you can manually write or edit them for precise control.

## Schema Structure

```sql
CREATE TABLE table_name (
  id INTEGER PRIMARY KEY,
  field1 TYPE1,
  field2 TYPE2 REFERENCES other_table(id),
  field3_symbol INTEGER REFERENCES field3_symbol(id)
);
```

## Supported Field Types

- `INTEGER`: Numeric integers
- `REAL`: Floating-point numbers
- `TEXT`: String data
- `BOOLEAN`: True/false values
- `JSON`: Nested arrays or objects

## Symbol Tables

Symbol tables optimize storage for fields with repeated values. They store each unique value once and reference it by ID.

### Manual Symbol Table Definition

```sql
CREATE TABLE main (
  id INTEGER PRIMARY KEY,
  category_symbol INTEGER REFERENCES category_symbol(id)
);

CREATE TABLE category_symbol (
  id INTEGER PRIMARY KEY,
  value TEXT UNIQUE
);
```

The suffix `_symbol` tells JSQL that this field references a symbol table.

### Auto-Generated Symbol Tables

When using `analyze` or `import`, JSQL automatically creates symbol tables for fields with many repeated values (typically when unique values < 20% of total rows). For example:

```json
{"category": "electronics"}
{"category": "clothing"}
{"category": "electronics"}
```

Generates:
```sql
CREATE TABLE main (
  id INTEGER PRIMARY KEY,
  category_symbol INTEGER REFERENCES category_symbol(id)
);

CREATE TABLE category_symbol (
  id INTEGER PRIMARY KEY,
  value TEXT UNIQUE
);
```

### Symbol Tables for JSON Fields

Symbol tables also work for repeated JSON structures (arrays/objects):

```json
{"tags": ["electronics", "sale"]}
{"tags": ["clothing"]}
{"tags": ["electronics", "sale"]}
```

Creates a symbol table with JSON strings:
```sql
CREATE TABLE tags_symbol (
  id INTEGER PRIMARY KEY,
  value TEXT UNIQUE  -- Stores JSON as text: ["electronics", "sale"]
);
```

## Nested Objects with Foreign Keys

For nested objects:

```sql
CREATE TABLE main (
  id INTEGER PRIMARY KEY,
  meta_id INTEGER REFERENCES meta(id)
);

CREATE TABLE meta (
  id INTEGER PRIMARY KEY,
  city TEXT,
  region TEXT
);
```

This handles JSON like:
```json
{"meta": {"city": "New York", "region": "NY"}}
```

## Table Dependencies

Tables are created in dependency order, with referenced tables first. JSQL uses topological sorting to resolve these dependencies.

## Editing Auto-Generated Schemas

When modifying an auto-generated schema:

1. Add/remove/rename columns as needed
2. Change column types to match your requirements
3. Add/modify foreign key relationships
4. Create additional symbol tables for high-cardinality fields
5. Force specific fields to use symbol tables by renaming them with `_symbol` suffix
6. Ensure table order respects dependencies

## Example Workflow

1. Generate initial schema:
   ```
   go run ./... analyze --input data.json > schema.sql
   ```

2. Edit schema.sql to customize structure

3. Create and load database:
   ```
   go run ./... create-db --db mydata.db --schema schema.sql
   go run ./... load --db mydata.db --schema schema.sql --input data.json
   ```

4. Verify with dump:
   ```
   go run ./... dump --schema schema.sql --db mydata.db
   ```

## Performance Tips

- Use symbol tables for any field with many repeated values
- For fields that might grow to have many unique values but start small, manually add `_symbol` suffix
- For large datasets, consider running `analyze` on a representative sample with `--sample 1000`
