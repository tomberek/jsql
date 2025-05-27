package main

import (
	"flag"
	"fmt"
	"os"
)

// Command-line handlers

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