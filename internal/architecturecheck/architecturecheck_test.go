// Rationale: the architecture checker is a repository boundary, so its schema, ratchets, AST rules, lexical rules, and stable output need hermetic contract coverage.
package architecturecheck

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestReadBaselineStrictness(t *testing.T) {
	tests := []struct {
		name string
		json string
		want string
	}{
		{"unknown", baselineJSON(`, "extra": 1`), "unknown field"},
		{"duplicate", strings.Replace(baselineJSON(""), `"version":1`, `"version":1,"version":1`, 1), "duplicate"},
		{"trailing", baselineJSON("") + "{}", "trailing"},
		{
			"unsorted",
			strings.Replace(
				baselineJSON(""),
				`"oversized_files":[]`,
				`"oversized_files":[{"path":"internal/z.go","lines":601},{"path":"internal/a.go","lines":601}]`,
				1,
			),
			"sorted",
		},
		{"bad limit", strings.Replace(baselineJSON(""), `"production":600`, `"production":1`, 1), "limits"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeFixture(t, "baseline.json", test.json)
			_, err := ReadBaseline(context.Background(), path)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(test.want)) {
				t.Fatalf("ReadBaseline() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCheckRatchetsAndFrozenTotals(t *testing.T) {
	baseline := validBaseline()
	root := checkRoot(t)
	writeFixtureAt(t, root, "internal/app/base.go", "package app\n\nvar A = 1\n")
	findings, err := Check(context.Background(), root, baseline)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Rule != "frozen-total-drift" {
		t.Fatalf("findings = %+v", findings)
	}
	writeFixtureAt(t, root, "internal/foo/a.go", "package foo\n"+strings.Repeat("\n", 600))
	findings, err = Check(context.Background(), root, validBaseline())
	if err != nil {
		t.Fatal(err)
	}
	if !hasRule(findings, "missing-oversized-baseline") {
		t.Fatalf("findings = %+v", findings)
	}
}

func TestCheckGoRulesAndExceptions(t *testing.T) {
	root := checkRoot(t)
	writeFixtureAt(
		t,
		root,
		"internal/adapters/demo/demo.go",
		"package demo\n\nimport \"example.com/project/internal/infra/etcd\"\nimport \"unsafe\"\ntype Contract interface { Run() }\ntype Alias = Contract\nfunc New() Contract { return nil }\nfunc NewAlias() Alias { return nil }\n",
	)
	findings, err := Check(context.Background(), root, validBaseline())
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range []string{"layer-import", "unsafe-import", "interface-constructor"} {
		if !hasRule(findings, rule) {
			t.Fatalf("missing %s in %+v", rule, findings)
		}
	}
	baseline := validBaseline()
	baseline.LegacyFindings = []LegacyFinding{
		{Path: "internal/adapters/demo/demo.go", Rule: "interface-constructor", Subject: "New", Reason: "legacy seam"},
	}
	findings, err = Check(context.Background(), root, baseline)
	if err != nil {
		t.Fatal(err)
	}
	if countRule(findings, "interface-constructor") != 1 || hasRule(findings, "stale-legacy-finding") {
		t.Fatalf("exact exception not applied: %+v", findings)
	}
}

func TestCheckTypeScriptLexicalRulesAndDirectories(t *testing.T) {
	root := checkRoot(t)
	writeFixtureAt(
		t,
		root,
		"console/src/features/types/code.ts",
		"// as any\nconst a = 'as unknown as X';\nconst b = `text ${value as any}`;\nconst c = value as any;\nconst d = value as unknown as Thing;\nconst e = <any>value;\nconst r = /as any/;\nfunction regex() { return /as any/ }\n",
	)
	writeFixtureAt(t, root, "console/src/features/view.tsx", "export const View = () => <p>as unknown as</p>\n")
	findings, err := Check(context.Background(), root, validBaseline())
	if err != nil {
		t.Fatal(err)
	}
	if !hasRule(findings, "catch-all-directory") || countRule(findings, "ts-unsafe-assertion") != 4 {
		t.Fatalf("findings = %+v", findings)
	}
	for _, finding := range findings {
		if finding.Rule == "ts-unsafe-assertion" && finding.Line == 1 {
			t.Fatalf("comment produced finding: %+v", finding)
		}
	}
}

func TestCheckUnsafeMappingRules(t *testing.T) {
	root := checkRoot(t)
	writeFixtureAt(t, root, "internal/controller/mapping.go", `package controller

import "encoding/json"

type contract interface{ run() }
type concrete struct{}
func (concrete) run() {}
type response struct { Data map[string]any }

func convert(value contract, input any) {
	_ = value.(concrete)
	encoded, _ := json.Marshal(input)
	var output concrete
	_ = json.Unmarshal(encoded, &output)
}
`)
	findings, err := Check(context.Background(), root, validBaseline())
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range []string{"local-concrete-recovery", "open-model-field", "json-roundtrip-conversion"} {
		if !hasRule(findings, rule) {
			t.Fatalf("missing %s in %+v", rule, findings)
		}
	}
}

func TestCheckImportMatrixAndReflectExceptions(t *testing.T) {
	root := checkRoot(t)
	writeFixtureAt(
		t,
		root,
		"internal/agent/bad.go",
		"package agent\nimport _ \"example.com/project/internal/controller\"\n",
	)
	writeFixtureAt(t, root, "pkg/errs/bad.go", "package errs\nimport _ \"example.com/project/internal/common/ids\"\n")
	writeFixtureAt(
		t,
		root,
		"internal/controller/schema.go",
		"package controller\nimport \"reflect\"\nvar _ = reflect.TypeOf(0)\n",
	)
	findings, err := Check(context.Background(), root, validBaseline())
	if err != nil {
		t.Fatal(err)
	}
	if countRule(findings, "layer-import") != 2 || !hasRule(findings, "reflect-import") {
		t.Fatalf("findings = %+v", findings)
	}
	baseline := validBaseline()
	baseline.LegacyFindings = []LegacyFinding{
		{
			Path:    "internal/controller/schema.go",
			Rule:    "reflect-import",
			Subject: "reflect",
			Reason:  "legacy schema boundary",
		},
	}
	findings, err = Check(context.Background(), root, baseline)
	if err != nil {
		t.Fatal(err)
	}
	if hasRule(findings, "reflect-import") || hasRule(findings, "stale-legacy-finding") {
		t.Fatalf("reflect exception not applied: %+v", findings)
	}
}

func TestWriteFindingsIsDeterministic(t *testing.T) {
	findings := []Finding{
		{Path: "b", Line: 2, Column: 1, Rule: "z", Message: "last"},
		{Path: "a", Line: 1, Column: 1, Rule: "a", Message: "first"},
	}
	var output bytes.Buffer
	if err := WriteFindings(&output, findings); err != nil {
		t.Fatal(err)
	}
	var decoded []Finding
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, []Finding{findings[1], findings[0]}) {
		t.Fatalf("output = %+v", decoded)
	}
}

func validBaseline() Baseline {
	return Baseline{
		Version:        1,
		Limits:         Limits{Production: 600, Test: 1000},
		OversizedFiles: []OversizedFile{},
		FrozenTotals:   []FrozenTotal{{Path: "internal/app", Lines: 1}, {Path: "internal/infra/etcd", Lines: 1}},
		LegacyFindings: []LegacyFinding{},
	}
}

func baselineJSON(suffix string) string {
	return `{"version":1,"limits":{"production":600,"test":1000},"oversized_files":[],"frozen_totals":[{"path":"internal/app","lines":1},{"path":"internal/infra/etcd","lines":1}],"legacy_findings":[]` + suffix + `}`
}

func checkRoot(t *testing.T) string {
	root := t.TempDir()
	writeFixtureAt(t, root, "go.mod", "module example.com/project\n\ngo 1.26\n")
	writeFixtureAt(t, root, "internal/app/base.go", "package app\n")
	writeFixtureAt(t, root, "internal/infra/etcd/base.go", "package etcd\n")
	return root
}

func writeFixture(t *testing.T, name, content string) string {
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeFixtureAt(t *testing.T, root, name, content string) {
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func hasRule(findings []Finding, rule string) bool {
	for _, finding := range findings {
		if finding.Rule == rule {
			return true
		}
	}
	return false
}

func countRule(findings []Finding, rule string) int {
	count := 0
	for _, finding := range findings {
		if finding.Rule == rule {
			count++
		}
	}
	return count
}

func TestFindingSortOrder(t *testing.T) {
	findings := []Finding{
		{Path: "a", Line: 2, Column: 1},
		{Path: "a", Line: 1, Column: 2},
		{Path: "a", Line: 1, Column: 1},
	}
	sortFindings(findings)
	if !sort.SliceIsSorted(findings, func(i, j int) bool {
		return findings[i].Line < findings[j].Line ||
			findings[i].Line == findings[j].Line && findings[i].Column <= findings[j].Column
	}) {
		t.Fatalf("findings not sorted: %+v", findings)
	}
}
