package app

import (
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func TestValidateScriptCreationInputUsesStableOwnersAndClosedHook(t *testing.T) {
	// Rationale: Controller input must carry stable ownership while leaving the Service label derived.
	input := apiTypes.ScriptCreate{
		EnvironmentID: ids.New(ids.KindEnvironment), Slug: "migrate", ServiceID: ids.New(ids.KindService),
		Body: "php artisan migrate --force", When: "pre-deploy",
	}
	if err := validateScriptCreationInput(input); err != nil {
		t.Fatalf("validateScriptCreationInput(): %v", err)
	}
	input.When = "scheduled"
	if err := validateScriptCreationInput(input); err == nil {
		t.Fatal("validateScriptCreationInput() accepted an unknown hook")
	}
}

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
