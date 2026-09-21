package scriptdefinition

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: context-only edits preserve the body generation, omission preserves
// grants, replacement drops old grants, and inheritance is a complete reset.
func TestScriptExecutionCreateEditAndResponse(t *testing.T) {
	input := apiTypes.ScriptCreate{
		EnvironmentID: ids.New(ids.KindEnvironment), ServiceID: ids.New(ids.KindService),
		Slug: "prepare", Body: "echo prepare", When: "pre-deploy", Execution: executionInput(),
	}
	if err := ValidateCreation(input); err != nil {
		t.Fatal(err)
	}
	desired := CreateDesired(input, ids.New(ids.KindScript), "consumer")
	if desired.Execution == nil || desired.Execution.Image != input.Execution.Image ||
		len(desired.Execution.Volumes) != 1 || desired.Execution.Volumes[0].ReadOnly {
		t.Fatalf("create lost execution: %#v", desired.Execution)
	}
	record, err := testscripts.NewRecord(input.EnvironmentID, input.ServiceID, desired)
	if err != nil {
		t.Fatal(err)
	}
	unchanged := EditDesired(desired, "renamed", apiTypes.ScriptEdit{})
	if unchanged.Execution == nil || unchanged.Execution.Image != desired.Execution.Image {
		t.Fatal("omission reset execution")
	}
	replacement := executionInput()
	replacement.Volumes, replacement.EntryIDs = nil, nil
	patch := apiTypes.ScriptEdit{Execution: replacement}
	if err := ValidateEdit(patch); err != nil {
		t.Fatal(err)
	}
	edited := EditDesired(desired, "consumer", patch)
	if edited.Execution == nil || len(edited.Execution.Volumes) != 0 || len(edited.Execution.EntryIDs) != 0 {
		t.Fatalf("execution patch merged grants: %#v", edited.Execution)
	}
	updated, err := testscripts.ReplaceDesired(record, edited)
	if err != nil || updated.ActiveGeneration != record.ActiveGeneration ||
		updated.Desired.Body != record.Desired.Body {
		t.Fatalf("context-only edit changed body generation: %#v, %v", updated, err)
	}
	response := Response(record)
	if response.Execution.Mode != "explicit" ||
		response.Execution.Volumes[0].VolumeID != input.Execution.Volumes[0].VolumeID ||
		response.Execution.EntryIDs[0] != input.Execution.EntryIDs[0] ||
		response.Execution.Volumes[0].ReadOnly {
		t.Fatalf("response lost grants: %#v", response.Execution)
	}
	reset := EditDesired(
		desired,
		"consumer",
		apiTypes.ScriptEdit{Execution: &apiTypes.ScriptExecution{Mode: "inherited"}},
	)
	if reset.Execution != nil || Response(testscripts.Record{Desired: reset}).Execution.Mode != "inherited" {
		t.Fatal("reset did not expose canonical inheritance")
	}
}

// Rationale: public writes validate format without probing resource availability,
// but typed callers cannot bypass the same immutable-image and safe-grant policy.
func TestScriptExecutionWriteValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*apiTypes.ScriptExecution)
	}{
		{"mutable image", func(execution *apiTypes.ScriptExecution) { execution.Image = "setup:latest" }},
		{"named user", func(execution *apiTypes.ScriptExecution) { execution.User = "root" }},
		{"invalid volume", func(execution *apiTypes.ScriptExecution) { execution.Volumes[0].VolumeID = "data" }},
		{"reserved path", func(execution *apiTypes.ScriptExecution) { execution.Volumes[0].Target = "/bin" }},
		{"duplicate entry", func(execution *apiTypes.ScriptExecution) {
			execution.EntryIDs = append(execution.EntryIDs, execution.EntryIDs[0])
		}},
		{"mixed mode", func(execution *apiTypes.ScriptExecution) { execution.Mode = "inherited" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			execution := executionInput()
			test.mutate(execution)
			input := apiTypes.ScriptCreate{
				EnvironmentID: ids.New(ids.KindEnvironment),
				ServiceID:     ids.New(ids.KindService),
				Slug:          "prepare",
				Body:          "echo prepare",
				When:          string(core.ScriptHook("manual")),
				Execution:     execution,
			}
			if err := ValidateCreation(input); err == nil {
				t.Fatal("create accepted invalid context")
			}
			if err := ValidateEdit(apiTypes.ScriptEdit{Execution: execution}); err == nil {
				t.Fatal("edit accepted invalid context")
			}
		})
	}
}

func executionInput() *apiTypes.ScriptExecution {
	return &apiTypes.ScriptExecution{Mode: "explicit", Image: "setup@sha256:" + strings.Repeat("a", 64), User: "0:0",
		Volumes: []apiTypes.ScriptVolumeGrant{
			{VolumeID: ids.New(ids.KindVolume), Target: "/etc/tls", ReadOnly: false},
		},
		EntryIDs: []string{ids.New(ids.KindEnvEntry)}}
}
