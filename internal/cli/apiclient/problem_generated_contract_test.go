package apiclient

import (
	"reflect"
	"slices"
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
