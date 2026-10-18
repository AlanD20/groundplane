package attachplan

import (
	"bytes"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BACK-08: both accepted action types must preserve the captured password-only
// identity through native delivery, including after the decryption callback clears it.
func TestNativePasswordAttachAndDetachUseCapturedIdentity(t *testing.T) {
	for _, action := range []workexecution.Action{workexecution.ActionAttach, workexecution.ActionApply} {
		t.Run(string(action), func(t *testing.T) {
			fixture := newNativeAuthenticationFixture(t, core.BackingAuthenticationPassword)
			fixture.task.Action = action
			if action == workexecution.ActionApply {
				fixture.task.TargetID = fixture.generation.EnvironmentID
			}
			planner, err := NewProvisionPlanner(fixture.facts)
			if err != nil {
				t.Fatal(err)
			}
			input, err := planner.BuildCaptured(t.Context(), fixture.generation, fixture.task, fixture.item)
			if err != nil {
				t.Fatal(err)
			}
			defer attachexecution.ClearProvision(input)
			procedures, err := attachexecution.ProvisionProcedures(input)
			if err != nil || len(procedures) != 1 || fixture.facts.provisions != 1 {
				t.Fatalf("native provision = %v, %v; opens=%d", procedures, err, fixture.facts.provisions)
			}
			if procedures[0].Authentication != agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD ||
				procedures[0].Role != "default" || string(procedures[0].Password) != "captured-password" {
				t.Fatal("native provision lost its explicit password-only identity")
			}
			clear(procedures[0].Password)
			removal, err := NewRemovalPlanner(fixture.facts)
			if err != nil {
				t.Fatal(err)
			}
			cleanup, err := removal.Build(t.Context(), fixture.removal, proto.CloneOf(input.CapturedSource), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer attachexecution.ClearRemoval(cleanup)
			steps, err := attachexecution.CredentialProcedures(cleanup)
			if err != nil || len(steps) != 1 || fixture.facts.removals != 1 ||
				steps[0].Authentication != agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD ||
				steps[0].Role != "default" || string(steps[0].Password) != "captured-password" {
				t.Fatalf("native credential cleanup lost captured password: %v, %v", steps, err)
			}
			alias := cleanup.GetCredential().Password
			attachexecution.ClearRemoval(cleanup)
			if !bytes.Equal(alias, make([]byte, len(alias))) {
				t.Fatal("credential cleanup retained plaintext")
			}
		})
	}
}

// BACK-08/TASK-09: no-auth bindings settle in the Controller; they must not
// open credentials or manufacture a no-op Agent procedure on replay.
func TestNativeNoAuthenticationSettlesWithoutCredentialDelivery(t *testing.T) {
	fixture := newNativeAuthenticationFixture(t, core.BackingAuthenticationNone)
	if !fixture.generation.SettlesInController() {
		t.Fatal("no-auth binding requested host provisioning")
	}
	ready, err := fixture.generation.ControllerBinding(fixture.task.CreatedAt)
	if err != nil || ready.Status != core.AttachReady || !fixture.generation.MatchesControllerBinding(ready, 11) {
		t.Fatalf("Controller settlement = %v, %v", ready, err)
	}
	planner, err := NewProvisionPlanner(fixture.facts)
	if err != nil {
		t.Fatal(err)
	}
	if input, err := planner.BuildCaptured(t.Context(), fixture.generation, fixture.task, fixture.item); err == nil ||
		input != nil ||
		fixture.facts.provisions != 0 {
		t.Fatal("no-auth binding opened credentials or produced a host no-op")
	}
}

// TASK-09: neither edited captured bytes nor contradictory decrypted facts may
// acquire delivery authority under the original accepted work identity.
func TestNativeAuthenticationRejectsChangedCaptureAndFacts(t *testing.T) {
	fixture := newNativeAuthenticationFixture(t, core.BackingAuthenticationPassword)
	planner, err := NewProvisionPlanner(fixture.facts)
	if err != nil {
		t.Fatal(err)
	}
	changed := fixture.generation
	changed.GeneratedInputs.Ciphertext = append([]byte(nil), changed.GeneratedInputs.Ciphertext...)
	changed.GeneratedInputs.Ciphertext[0] ^= 1
	if input, err := planner.BuildCaptured(t.Context(), changed, fixture.task, fixture.item); err == nil ||
		input != nil ||
		fixture.facts.provisions != 0 {
		t.Fatal("modified captured envelope reached credential resolution")
	}
	changed = fixture.generation
	changed.Authentication = core.BackingAuthenticationUsernamePassword
	if input, err := planner.BuildCaptured(t.Context(), changed, fixture.task, fixture.item); err == nil ||
		input != nil ||
		fixture.facts.provisions != 0 {
		t.Fatal("changed authentication replayed the original work input")
	}
	fixture.facts.wrongMode = true
	if input, err := planner.BuildCaptured(t.Context(), fixture.generation, fixture.task, fixture.item); err == nil ||
		input != nil ||
		fixture.facts.provisions != 1 {
		t.Fatal("contradictory decrypted authentication acquired delivery authority")
	}
	removal, err := NewRemovalPlanner(fixture.facts)
	if err != nil {
		t.Fatal(err)
	}
	reference := &agentpb.WorkInputReference{InputId: fixture.item.Input.ID, Sha256: make([]byte, 32)}
	if input, err := removal.Build(t.Context(), fixture.removal, reference, nil); err == nil || input != nil ||
		fixture.facts.removals != 1 {
		t.Fatal("contradictory Detach authentication acquired cleanup authority")
	}
}
