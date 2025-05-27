package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
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

// --- BASIC Roundtrip Example from previous test --- //
func TestImportAllAndDump(t *testing.T) {
	const testJSON = `
{"name": "Alice", "age": 30, "meta": {"city":"Wonderland"}}
{"name": "Bob", "age": 31, "meta": {"city":"Builderland"}}
{"name": "Carol", "age": 25, "meta": {"city":"Charleston"}}
`
	dataPath := writeTempFile(t, "testdata.json", testJSON)
	defer removeFiles(dataPath)

	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "test-importall.db")
	ddlPath := filepath.Join(tmp, "test-importall.sql")
	binPath := filepath.Join(tmp, "clihelper")

	// Build CLI
	if out, err := exec.Command("go", "build", "-o", binPath, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	defer removeFiles(binPath)

	cmd := exec.Command(binPath, "import", "--input", dataPath, "--db", dbPath, "--schema", ddlPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("import: %v\n%s", err, out)
	}

	// Dump
	cmd = exec.Command(binPath, "dump", "--db", dbPath, "--schema", ddlPath)
	dumpOut, err := cmd.Output()
	if err != nil {
		t.Fatalf("dump: %v", err)
	}

	got := decodeAllLines(t, dumpOut)

	// Parse the source, to compare as data
	srcRaw := bytes.Split([]byte(testJSON), []byte{'\n'})
	var want []map[string]interface{}
	for _, l := range srcRaw {
		if len(bytes.TrimSpace(l)) == 0 {
			continue
		}
		var m map[string]interface{}
		json.Unmarshal(l, &m)
		want = append(want, m)
	}
	if !reflect.DeepEqual(normalizeJSON(got), normalizeJSON(want)) {
		t.Errorf("roundtrip mismatch\nGOT: %+v\nWANT: %+v", got, want)
	}
}

// --- ADVANCED ROUNDTRIP TEST: Arrays and nested objects --- //
func TestRoundtripArraysAndNesting(t *testing.T) {
	const testJSON = `
{"type": "A", "ids": [1,2,3], "sub": {"foo": "bar", "val": 5.5}}
{"type": "B", "ids": [], "sub": {"foo": "baz", "val": 7.1}}
{"type": "C", "ids": [17], "sub": {"foo": "qux", "val": 0}}
`
	dataPath := writeTempFile(t, "testarray.json", testJSON)
	defer removeFiles(dataPath)

	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "test-arrays.db")
	ddlPath := filepath.Join(tmp, "test-arrays.sql")
	binPath := filepath.Join(tmp, "cliarray")

	// Build CLI (again)
	if out, err := exec.Command("go", "build", "-o", binPath, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	defer removeFiles(binPath)

	// Import all
	cmd := exec.Command(binPath, "import", "--input", dataPath, "--db", dbPath, "--schema", ddlPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("import: %v\n%s", err, out)
	}

	// Dump
	cmd = exec.Command(binPath, "dump", "--db", dbPath, "--schema", ddlPath)
	dumpOut, err := cmd.Output()
	if err != nil {
		t.Fatalf("dump: %v", err)
	}

	got := decodeAllLines(t, dumpOut)

	// Parse input as data
	srcRaw := bytes.Split([]byte(testJSON), []byte{'\n'})
	var want []map[string]interface{}
	for _, l := range srcRaw {
		if len(bytes.TrimSpace(l)) == 0 {
			continue
		}
		var m map[string]interface{}
		json.Unmarshal(l, &m)
		want = append(want, m)
	}
	normGot := normalizeJSON(got)
	normWant := normalizeJSON(want)
	if !reflect.DeepEqual(normGot, normWant) {
		t.Errorf("array/nested roundtrip mismatch\nGOT: %+v\nWANT: %+v", normGot, normWant)
	}
}

// --- ADVANCED ROUNDTRIP TEST: Arrays and nested objects --- //
func TestRoundtripNixJSON(t *testing.T) {
	testJSON, err := os.ReadFile("nix.json")

	dataPath := writeTempFile(t, "testarray.json", string(testJSON))
	defer removeFiles(dataPath)

	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "test-arrays.db")
	ddlPath := filepath.Join(tmp, "test-arrays.sql")
	binPath := filepath.Join(tmp, "cliarray")

	// Build CLI (again)
	if out, err := exec.Command("go", "build", "-o", binPath, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	defer removeFiles(binPath)

	// Import all
	cmd := exec.Command(binPath, "import", "--input", dataPath, "--db", dbPath, "--schema", ddlPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("import: %v\n%s", err, out)
	}

	// Dump
	cmd = exec.Command(binPath, "dump", "--db", dbPath, "--schema", ddlPath)
	dumpOut, err := cmd.Output()
	if err != nil {
		t.Fatalf("dump: %v", err)
	}

	got := decodeAllLines(t, dumpOut)

	// Parse input as data
	srcRaw := bytes.Split([]byte(testJSON), []byte{'\n'})
	var want []map[string]interface{}
	for _, l := range srcRaw {
		if len(bytes.TrimSpace(l)) == 0 {
			continue
		}
		var m map[string]interface{}
		json.Unmarshal(l, &m)
		want = append(want, m)
	}
	normGot := normalizeJSON(got)
	normWant := normalizeJSON(want)
	if !reflect.DeepEqual(normGot, normWant) {
		t.Errorf("array/nested roundtrip mismatch\nGOT: %+v\nWANT: %+v", normGot, normWant)
	}
}
