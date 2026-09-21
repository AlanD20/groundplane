package scriptdefinition

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: a protected mutation cannot replay a different image, identity or
// resource grant; creation inheritance stays compatible with omitted defaults.
func TestScriptExecutionProtectedMutationIntent(t *testing.T) {
	base := apiTypes.ScriptCreate{
		Slug:      "prepare",
		Body:      "echo prepare",
		When:      "pre-deploy",
		Execution: executionInput(),
	}
	for _, test := range []struct {
		name   string
		mutate func(*apiTypes.ScriptExecution)
	}{
		{"mode", func(execution *apiTypes.ScriptExecution) { *execution = apiTypes.ScriptExecution{Mode: "inherited"} }},
		{"image", func(execution *apiTypes.ScriptExecution) { execution.Image = "other" + execution.Image }},
		{"user", func(execution *apiTypes.ScriptExecution) { execution.User = "1:1" }},
		{"volume", func(execution *apiTypes.ScriptExecution) { execution.Volumes[0].VolumeID = ids.New(ids.KindVolume) }},
		{"target", func(execution *apiTypes.ScriptExecution) { execution.Volumes[0].Target = "/output" }},
		{"access", func(execution *apiTypes.ScriptExecution) { execution.Volumes[0].ReadOnly = true }},
		{"entry", func(execution *apiTypes.ScriptExecution) { execution.EntryIDs[0] = ids.New(ids.KindEnvEntry) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := base
			execution := *base.Execution
			execution.Volumes = append([]apiTypes.ScriptVolumeGrant(nil), execution.Volumes...)
			execution.EntryIDs = append([]string(nil), execution.EntryIDs...)
			test.mutate(&execution)
			changed.Execution = &execution
			assertExecutionIntentMatch(t, CreateIntentBody(base), CreateIntentBody(changed), false)
			assertExecutionIntentMatch(t, EditIntentBody(apiTypes.ScriptEdit{Execution: base.Execution}),
				EditIntentBody(apiTypes.ScriptEdit{Execution: changed.Execution}), false)
		})
	}
	base.Execution = nil
	inherited := base
	inherited.Execution = &apiTypes.ScriptExecution{Mode: "inherited"}
	assertExecutionIntentMatch(t, CreateIntentBody(base), CreateIntentBody(inherited), true)
	assertExecutionIntentMatch(t, EditIntentBody(apiTypes.ScriptEdit{}),
		EditIntentBody(apiTypes.ScriptEdit{Execution: inherited.Execution}), false)
}

func assertExecutionIntentMatch(t *testing.T, left, right idempotentintent.Value, want bool) {
	t.Helper()
	protector, err := secretvalue.NewProtector(intentCipher{}, intentCipher{})
	if err != nil {
		t.Fatal(err)
	}
	lhs, rhs := protectIntent(t, protector, left), protectIntent(t, protector, right)
	defer lhs.Clear()
	defer rhs.Clear()
	match, err := idempotentintent.CompareProtected(context.Background(), protector, lhs, rhs)
	if err != nil || match != want {
		t.Fatalf("protected context match = %v, %v; want %v", match, err, want)
	}
}
