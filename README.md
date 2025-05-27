# JSON to SQL

```
go run ./... analyze --input some.json > schema
go run ./... create-db --db db --schema schema
go run ./... load --db db --schema schema --input some.json

# or to do it all in one go...
go run ./... import --db db --schema schema --input some.json

# dump
go run ./... dump --schema schema --db db
```
