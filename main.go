package main

import (
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

// Main entry point for the application
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, `Usage:
  %s analyze --input data.json [--sample N]
  %s create-db --schema ddl.sql --db my.db
  %s load --input data.json --db my.db --schema ddl.sql
  %s dump --db my.db --schema ddl.sql
  %s import --input data.json --db my.db [--schema ddl.sql]
`, os.Args[0], os.Args[0], os.Args[0], os.Args[0], os.Args[0])
		os.Exit(1)
	}
	
	// Dispatch to the appropriate command
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