package app

import (
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func TestScriptEditMutationIntentIncludesOnlyAuthoredFields(t *testing.T) {
	// Rationale: omitted PATCH fields must not collide with an explicit empty Script body.
	body := ""
	intent := scriptEditMutationIntent(
		ids.New(ids.KindScript), ids.New(ids.KindEnvironment), apiTypes.ScriptEdit{Body: &body},
	)
	wantBody := idempotentintent.Object(
		idempotentintent.Field{Name: "script", Value: idempotentintent.String("")},
	)
	if intent.method == "" || intent.route != scriptEditRoute || !reflect.DeepEqual(intent.body, wantBody) {
		t.Fatal("scriptEditMutationIntent() did not preserve the authored patch")
	}
}
