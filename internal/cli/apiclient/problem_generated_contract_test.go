package apiclient

import (
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
)

// Rationale: the generated CLI model must remain the closed RFC 7807 tuple;
// an open or extra field would let the client trust data the Controller never emits.
func TestGeneratedProblemModelHasExactFields(t *testing.T) {
	model := reflect.TypeFor[generated.Error]()
	fields := make([]string, 0, model.NumField())
	for index := range model.NumField() {
		fields = append(fields, model.Field(index).Tag.Get("json"))
	}
	slices.Sort(fields)
	want := []string{"code", "detail", "status", "title", "type"}
	if !slices.Equal(fields, want) {
		t.Fatalf("generated Error JSON fields = %v, want %v", fields, want)
	}

	var problem generated.Error
	var _ string = problem.Type
	var _ string = problem.Title
	var _ int64 = problem.Status
	var _ string = problem.Detail
	var _ string = problem.Code
}

// Rationale: every generated parser must establish exactly-one response-body
// ownership before the bounded reader can return through any error branch.
func TestGeneratedParsersRegisterCloseBeforeRead(t *testing.T) {
	generatedSource, err := os.ReadFile("generated/client.gen.go")
	if err != nil {
		t.Fatalf("read generated client source: %v", err)
	}

	const parserMarker = "\nfunc Parse"
	const firstStatements = "\tdefer func() { _ = rsp.Body.Close() }()\n\tbodyBytes, err := problemresponse.Read(rsp)\n"
	parsers := strings.Split(string(generatedSource), parserMarker)[1:]
	if len(parsers) != 113 {
		t.Fatalf("generated parser count = %d, want 113", len(parsers))
	}
	for index, parser := range parsers {
		bodyStart := strings.Index(parser, "{\n")
		if bodyStart < 0 || !strings.HasPrefix(parser[bodyStart+2:], firstStatements) {
			t.Fatalf("generated parser %d does not register Close before Read", index+1)
		}
	}
	if reads := strings.Count(string(generatedSource), "problemresponse.Read(rsp)"); reads != len(parsers) {
		t.Fatalf("generated problemresponse.Read count = %d, want %d", reads, len(parsers))
	}
}
