package swarmcheck

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Rationale: scoped regressions must retain the adjacent constraint explanation required by standards.
func TestScopedTestsHaveRationaleHeaders(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve rationale test path")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	for _, relativeDirectory := range []string{
		"cmd/swarm-check",
		"internal/swarmcheck",
		"internal/swarmgit",
	} {
		directory := filepath.Join(repositoryRoot, relativeDirectory)
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			filePath := filepath.Join(directory, entry.Name())
			file, err := parser.ParseFile(
				token.NewFileSet(),
				filePath,
				nil,
				parser.ParseComments,
			)
			if err != nil {
				t.Fatal(err)
			}
			for _, declaration := range file.Decls {
				function, isFunction := declaration.(*ast.FuncDecl)
				if !isFunction || !strings.HasPrefix(function.Name.Name, "Test") {
					continue
				}
				if function.Doc == nil || !strings.HasPrefix(function.Doc.Text(), "Rationale:") {
					t.Errorf("%s: %s lacks an adjacent // Rationale: header", filePath, function.Name)
				}
			}
		}
	}
}
