package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Helper for tests
func writeTempFile(t *testing.T, prefix, content string) string {
	f, err := os.CreateTemp("", prefix)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(content)
	f.Close()
	return f.Name()
}

func removeFiles(paths ...string) {
	for _, p := range paths {
		os.Remove(p)
	}
}

// Accepts slices of map[string]interface{} where potential id fields
// are ignored, and objects may appear in any order
func normalizeJSON(objs []map[string]interface{}) []map[string]interface{} {
	for _, obj := range objs {
		delete(obj, "id")
		// Remove nested id fields for one-object subfields
		for k, v := range obj {
			if m, ok := v.(map[string]interface{}); ok {
				delete(m, "id")
				obj[k] = m
			}
		}
	}
	sort.Slice(objs, func(i, j int) bool {
		li := ""
		lj := ""
		if n, ok := objs[i]["name"].(string); ok {
			li = n
		} else if n, ok := objs[i]["type"].(string); ok {
			li = n
		}
		if n, ok := objs[j]["name"].(string); ok {
			lj = n
		} else if n, ok := objs[j]["type"].(string); ok {
			lj = n
		}
		return li < lj
	})
	return objs
}

// Helper to parse multiple lines of JSON objects to []map[string]interface{}
func decodeAllLines(t *testing.T, b []byte) []map[string]interface{} {
	var res []map[string]interface{}
	dec := json.NewDecoder(bytes.NewReader(b))
	for dec.More() {
		var m map[string]interface{}
		if err := dec.Decode(&m); err != nil {
			t.Fatalf("decode: %v", err)
		}
		res = append(res, m)
	}
	return res
}

// Common roundtrip test function that processes JSON data through SQLite and back
func roundtripTest(t *testing.T, testJSON string, testFile string, testName string, validateSchema func(string, *testing.T)) {
	var content []byte
	var err error
	
	if testJSON != "" {
		// Use inline JSON
		content = []byte(testJSON)
	} else if testFile != "" {
		// Read from file
		_, err := os.Stat(testFile)
		if os.IsNotExist(err) {
			t.Skipf("%s not found, skipping test", testFile)
			return
		}
		
		content, err = os.ReadFile(testFile)
		if err != nil {
			t.Fatalf("Failed to read test file: %v", err)
			return
		}
	} else {
		t.Fatal("Either testJSON or testFile must be provided")
		return
	}
	
	// Create temp file if using inline JSON
	var dataPath string
	if testJSON != "" {
		dataPath = writeTempFile(t, "test-"+testName+".json", testJSON)
		defer removeFiles(dataPath)
	} else {
		dataPath = testFile
	}

	// Setup paths
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "test-"+testName+".db")
	ddlPath := filepath.Join(tmp, "test-"+testName+".sql")
	binPath := filepath.Join(tmp, "cli-"+testName)

	// Build CLI
	if out, err := exec.Command("go", "build", "-o", binPath, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	defer removeFiles(binPath)

	// Import data
	cmd := exec.Command(binPath, "import", 
		"--input", dataPath, 
		"--db", dbPath, 
		"--schema", ddlPath)
	
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("import: %v\n%s", err, out)
	}

	// Examine schema if validator provided
	if validateSchema != nil {
		schemaDDL, err := os.ReadFile(ddlPath)
		if err != nil {
			t.Fatalf("Failed to read schema: %v", err)
		}
		validateSchema(string(schemaDDL), t)
	}

	// Dump data
	cmd = exec.Command(binPath, "dump", "--db", dbPath, "--schema", ddlPath)
	dumpOut, err := cmd.Output()
	if err != nil {
		t.Fatalf("dump: %v", err)
	}

	// Parse output
	got := decodeAllLines(t, dumpOut)

	// Parse original input
	srcRaw := bytes.Split(content, []byte{'\n'})
	var want []map[string]interface{}
	for _, l := range srcRaw {
		if len(bytes.TrimSpace(l)) == 0 {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal(l, &m); err != nil {
			t.Fatalf("Failed to unmarshal source JSON: %v", err)
		}
		want = append(want, m)
	}

	// Compare data
	normGot := normalizeJSON(got)
	normWant := normalizeJSON(want)

	// Check record count
	if len(normGot) != len(normWant) {
		t.Errorf("Record count mismatch: got %d, want %d", len(normGot), len(normWant))
	}

	// Deep comparison
	if !reflect.DeepEqual(normGot, normWant) {
		// Detailed comparison to find differences
		for i, w := range normWant {
			if i >= len(normGot) {
				t.Errorf("Missing record at index %d", i)
				continue
			}
			g := normGot[i]
			
			// Compare each field
			for k, wv := range w {
				gv, exists := g[k]
				if !exists {
					t.Errorf("Record %d: Missing field %q", i, k)
					continue
				}
				
				// Special handling for deeply nested structures
				if !reflect.DeepEqual(gv, wv) {
					t.Errorf("Record %d, field %q: values don't match\nGot: %#v\nWant: %#v", 
						i, k, gv, wv)
				}
			}
		}
		
		t.Errorf("%s roundtrip failed", testName)
	} else {
		t.Logf("Successfully preserved %d records through SQLite roundtrip for %s test", 
			len(normWant), testName)
	}
}

// --- BASIC Roundtrip Example --- //
func TestImportAllAndDump(t *testing.T) {
	const testJSON = `
{"name": "Alice", "age": 30, "meta": {"city":"Wonderland"}}
{"name": "Bob", "age": 31, "meta": {"city":"Builderland"}}
{"name": "Carol", "age": 25, "meta": {"city":"Charleston"}}
`
	roundtripTest(t, testJSON, "", "basic", nil)
}

// --- ADVANCED ROUNDTRIP TEST: Arrays and nested objects --- //
func TestRoundtripArraysAndNesting(t *testing.T) {
	const testJSON = `
{"type": "A", "ids": [1,2,3], "sub": {"foo": "bar", "val": 5.5}}
{"type": "B", "ids": [], "sub": {"foo": "baz", "val": 7.1}}
{"type": "C", "ids": [17], "sub": {"foo": "qux", "val": 0}}
`
	roundtripTest(t, testJSON, "", "arrays", nil)
}

// --- ADVANCED ROUNDTRIP TEST: External Nix JSON --- //
func TestRoundtripNixJSON(t *testing.T) {
	validateSchema := func(schema string, t *testing.T) {
		tableCount := strings.Count(schema, "CREATE TABLE")
		t.Logf("Nix JSON schema contains %d tables", tableCount)
	}
	
	roundtripTest(t, "", "nix.json", "nix", validateSchema)
}

// --- SIMPLE ROUNDTRIP TEST --- //
func TestRoundtripSimpleStructures(t *testing.T) {
	validateSchema := func(schema string, t *testing.T) {
		tableCount := strings.Count(schema, "CREATE TABLE")
		t.Logf("Schema contains %d tables", tableCount)
	}
	
	roundtripTest(t, "", "test_simple.json", "simple", validateSchema)
}

// --- MODERATE COMPLEXITY ROUNDTRIP TEST --- //
func TestRoundtripModerateStructures(t *testing.T) {
	validateSchema := func(schema string, t *testing.T) {
		tableCount := strings.Count(schema, "CREATE TABLE")
		t.Logf("Schema contains %d tables", tableCount)
	}
	
	roundtripTest(t, "", "test_moderate.json", "moderate", validateSchema)
}

// --- HIGH COMPLEXITY ROUNDTRIP TEST --- //
func TestRoundtripHighComplexity(t *testing.T) {
	validateSchema := func(schema string, t *testing.T) {
		tableCount := strings.Count(schema, "CREATE TABLE")
		t.Logf("Schema contains %d tables for highly complex data", tableCount)
		
		indexCount := strings.Count(schema, "CREATE INDEX")
		t.Logf("Schema contains %d indexes", indexCount)
	}
	
	roundtripTest(t, "", "test_high_revised.json", "high-complex", validateSchema)
}
