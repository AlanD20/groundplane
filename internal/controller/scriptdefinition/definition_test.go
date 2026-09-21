package scriptdefinition

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func TestScriptResponseProjectsOnlyThePublicContract(t *testing.T) {
	// Rationale: the public response exposes both stable references and mutable operator labels.
	record, err := testscripts.NewRecord(ids.New(ids.KindEnvironment), ids.New(ids.KindService), core.Script{
		ID: ids.New(ids.KindScript), Slug: "migrate", ServiceName: "api",
		Body: "php artisan migrate --force", When: core.ScriptHook("manual"),
	})
	if err != nil {
		t.Fatalf("NewScriptRecord(): %v", err)
	}
	response := Response(record)
	if response.ID != record.Desired.ID || response.EnvironmentID != record.EnvironmentID ||
		response.Slug != "migrate" || response.ServiceID != record.ServiceID || response.ServiceName != "api" ||
		response.Body != record.Desired.Body || response.When != "manual" {
		t.Fatalf("Response() = %#v", response)
	}
}

func TestValidateScriptCreationInputUsesStableOwnersAndClosedHook(t *testing.T) {
	// Rationale: Controller input must carry stable ownership while leaving the Service label derived.
	input := apiTypes.ScriptCreate{
		EnvironmentID: ids.New(ids.KindEnvironment), Slug: "migrate", ServiceID: ids.New(ids.KindService),
		Body: "php artisan migrate --force", When: "pre-deploy",
	}
	if err := ValidateCreation(input); err != nil {
		t.Fatalf("ValidateCreation(): %v", err)
	}
	input.When = "scheduled"
	if err := ValidateCreation(input); err == nil {
		t.Fatal("ValidateCreation() accepted an unknown hook")
	}
}

// Rationale: all Script mutation/read projections retain numeric order, including
// a deliberate reset to zero; omitted edits preserve the previously saved value.
func TestScriptOrderCreateEditResponse(t *testing.T) {
	input := apiTypes.ScriptCreate{Slug: "migrate", Body: "echo migrate", When: "pre-deploy", Order: 65535}
	desired := CreateDesired(input, ids.New(ids.KindScript), "api")
	if desired.Order != input.Order {
		t.Errorf("created order = %d", desired.Order)
	}
	desired.Order = 65535
	if got := EditDesired(desired, "renamed", apiTypes.ScriptEdit{}); got.Order != 65535 {
		t.Errorf("omission reset order to %d", got.Order)
	}
	zero := uint16(0)
	patch := apiTypes.ScriptEdit{Order: &zero}
	if err := ValidateEdit(patch); err != nil {
		t.Errorf("order-only reset rejected: %v", err)
	}
	edited := EditDesired(desired, "api", patch)
	if edited.Order != 0 || edited.ID != desired.ID || edited.Body != desired.Body {
		t.Errorf("edited Script = %#v", edited)
	}
	if got := Response(testscripts.Record{Desired: desired}).Order; got != 65535 {
		t.Errorf("response order = %d", got)
	}
}
