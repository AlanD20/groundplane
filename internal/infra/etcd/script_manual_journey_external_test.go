package etcd_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// Rationale: the manual path must consume actual persisted serving Release and
// desired inputs, not a hand-populated ScriptExecutionSources with fake fences.
// Release and Script runtime acknowledgements remain supplied hermetic evidence.
func TestManualScriptServingReleaseToTerminalSourceJourney(t *testing.T) {
	testManualScriptServingReleaseJourney(t, manualJourneyNoEntry)
}

// Rationale: Entry bytes must travel from an immutable stored generation to the
// private assignment, including Retry, without entering the persisted plan.
func TestManualScriptServingReleaseEntryJourney(t *testing.T) {
	testManualScriptServingReleaseJourney(t, manualJourneyPlainEnv)
}

// Rationale: encrypted file generations must retain their ciphertext authority
// while only private assignments carry plaintext and sealed file permissions.
func TestManualScriptServingReleaseSecretFileJourney(t *testing.T) {
	testManualScriptServingReleaseJourney(t, manualJourneySecretFile)
}

// Rationale: a reusable Secret cannot be removed while a manual execution
// retains its source, including the unstarted Retry window.
func TestManualScriptServingReleaseSecretReferenceJourney(t *testing.T) {
	testManualScriptServingReleaseJourney(t, manualJourneySecretReference)
}

// Rationale: valid Blueprint Secret keys must resolve to stable source owners
// before manual publication, just as stable Secret ids do.
func TestManualScriptServingReleaseSecretKeyJourney(t *testing.T) {
	testManualScriptServingReleaseJourney(t, manualJourneySecretKey)
}

type manualJourneyEntryKind uint8

const (
	manualJourneyNoEntry manualJourneyEntryKind = iota
	manualJourneyPlainEnv
	manualJourneySecretFile
	manualJourneySecretReference
	manualJourneySecretKey
)

func testManualScriptServingReleaseJourney(t *testing.T, entryKind manualJourneyEntryKind) {
	withEntry := entryKind != manualJourneyNoEntry
	secretReference := entryKind == manualJourneySecretReference || entryKind == manualJourneySecretKey
	secretFile := entryKind == manualJourneySecretFile || secretReference
	testBlueprintExecutedArtifactConfigured(t, false, false, func(project *composetypes.Project) {
		service := project.Services["api"]
		service.NetworkMode = ""
		service.User = "1000:1000"
		project.Services["api"] = service
	}, func(fixture *etcd.ExecutedArtifactFixture, resolver *controller.TaskPlanResolver,
		_ etcd.ReleaseRenderInput, intent domain.Intent, _ *agentpb.ComposeArtifact) {
		ctx := context.Background()
		const entryValue = "manual-entry-private-artifact-test-value"
		var materializer *controller.TaskMaterializationResolver
		var entryRecord etcd.EntryRecord
		var secretID string
		if withEntry {
			cipher := unexpectedManualJourneyCipher{t: t}
			protector, err := secretvalue.NewProtector(cipher, cipher)
			if err != nil {
				t.Fatal(err)
			}
			var ciphertext []byte
			if secretFile {
				protector, ciphertext = manualJourneyEncryptedValue(t, entryValue)
				defer clear(ciphertext)
			}
			if secretReference {
				plaintext := []byte(entryValue)
				envelope, err := protector.Seal(ctx, plaintext)
				clear(plaintext)
				if err != nil {
					t.Fatal(err)
				}
				secretCiphertext := envelope.Ciphertext()
				envelope.Clear()
				if bytes.Equal(secretCiphertext, ciphertext) {
					t.Fatal("reusable Secret and Entry must have distinguishable ciphertext authority")
				}
				secretID = fixture.CreateManualJourneySecret(t, secretCiphertext)
				clear(secretCiphertext)
			}
			secretReference := secretID
			if entryKind == manualJourneySecretKey {
				secretReference = "manual-journey"
			}
			values, secrets, entry := fixture.PublishManualJourneyEntry(t, entryValue, ciphertext, secretReference)
			entryRecord = entry
			materializer, err = controller.NewTaskMaterializationResolver(
				fixture.Hierarchy,
				values,
				secrets,
				resolver,
				protector,
			)
			if err != nil {
				t.Fatal(err)
			}
		}
		scripts, task := fixture.CreateManualScript(t, intent.ServiceID)
		sources, err := scripts.LoadExecutionSources(ctx, fixture.Ledger, task.Target)
		if err != nil {
			t.Fatal(err)
		}
		if sources.Release.Intent.ID != intent.ID ||
			sources.Release.Intent.CandidateWorkload != intent.CandidateWorkload {
			t.Fatal("source discovery substituted the serving Release")
		}
		resolveArtifacts := scripts.ResolveScriptAssignmentArtifacts
		var prepared controller.ScriptRunnerPreparation
		if withEntry {
			service, err := controller.NewScriptArtifactService(scripts, materializer)
			if err != nil {
				t.Fatal(err)
			}
			preparation, err := controller.NewScriptRunnerPreparationService(
				service,
				&etcd.LocalAgentRepository{},
				retainedUnexpectedImageResolver{t: t},
			)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err = preparation.Prepare(ctx, sources)
			if err != nil {
				t.Fatal(err)
			}
			resolveArtifacts = service.ResolveScriptAssignmentArtifacts
		}
		plan, err := controller.BuildManualScriptPlan(ctx, controller.ManualScriptPlanInput{
			TaskID: task.ID, OperationID: task.OperationID, PlanID: task.PlanID, StepID: task.Steps[0].ID,
			ExecutionID: task.Params[etcd.ScriptExecutionIDParam], SnapshotID: ids.NewULID(), Sources: sources,
			Preparation: prepared,
		})
		if err != nil {
			t.Fatal(err)
		}
		bindings := plan.ScriptRunnerSnapshots[0].EntryBindings
		if withEntry {
			if len(bindings) != 1 || bindings[0].EntryId != entryRecord.Entry.ID ||
				bindings[0].ValueGenerationId != entryRecord.CurrentValueGenerationID {
				t.Fatal("binding substituted the published Entry generation")
			}
			if secretFile && (bindings[0].Kind != agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_FILE ||
				!bindings[0].Secret || bindings[0].FileTarget != "/run/secrets/manual-value" ||
				bindings[0].Uid != 1000 || bindings[0].Gid != 1000 || bindings[0].Mode != 0o600) {
				t.Fatal("secret file binding lost destination, owner, or private mode")
			}
		}
		encoded, err := proto.Marshal(plan)
		if err != nil || bytes.Contains(encoded, []byte(entryValue)) {
			t.Fatal("plan contains Entry plaintext or could not be encoded")
		}
		task.PlanHash, task.RenderGeneration = hex.EncodeToString(plan.PlanHash), int32(plan.RenderGeneration)
		if len(plan.ScriptRunnerSnapshots) != 1 ||
			plan.ScriptRunnerSnapshots[0].LocalImageId != intent.CandidateWorkload.LocalImageID ||
			plan.ScriptRunnerSnapshots[0].AppliedEnvironmentRenderGeneration != sources.DesiredProjection.Record.RenderGeneration {
			t.Fatal("runner did not bind serving image and current desired topology")
		}
		fixture.PublishManualScript(t, scripts, sources, task, plan)
		fixture.CheckManualJourneyServiceRemoval(t, intent.ServiceID, true)
		if withEntry {
			fixture.CheckManualJourneyEntryReference(t, entryRecord, true)
			fixture.CheckManualJourneyEntryRemoval(t, entryRecord.Entry.ID, true)
		}
		if secretID != "" {
			fixture.CheckManualJourneySecretDeletion(t, secretID, true)
		}
		agentID := ids.New(ids.KindAgent)
		assignment, found, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second))
		if err != nil || !found || assignment.Task.Record.ID != task.ID {
			t.Fatalf("manual claim = %t, %v", found, err)
		}
		admitted, err := scripts.GetScriptExecutionPlan(ctx, assignment.Task.Record)
		if err != nil || !proto.Equal(admitted, plan) {
			t.Fatalf("manual plan admission = %v", err)
		}
		if withEntry {
			checkManualJourneyValueSubstitution(t, scripts, materializer, assignment.Task.Record, admitted)
		}
		artifacts, err := resolveArtifacts(ctx, assignment.Task.Record, admitted)
		if err != nil || artifacts == nil || len(artifacts.Bodies) != 1 ||
			string(artifacts.Bodies[0].Body) != "exit 0" {
			t.Fatalf("manual body admission = %v", err)
		}
		clear(artifacts.Bodies[0].Body)
		checkManualJourneyEntry(t, artifacts, bindings, entryValue)
		assignment = fixture.RetryTimedOutManualScript(t, assignment, func() {
			fixture.CheckManualJourneyServiceRemoval(t, intent.ServiceID, true)
			if withEntry {
				fixture.CheckManualJourneyEntryReference(t, entryRecord, true)
				fixture.CheckManualJourneyEntryRemoval(t, entryRecord.Entry.ID, true)
			}
			if secretID != "" {
				fixture.CheckManualJourneySecretDeletion(t, secretID, true)
			}
		})
		if withEntry {
			fixture.CheckManualJourneyEntryReference(t, entryRecord, true)
		}
		if secretID != "" {
			fixture.CheckManualJourneySecretDeletion(t, secretID, true)
		}
		retried, err := scripts.GetScriptExecutionPlan(ctx, assignment.Task.Record)
		if err != nil || !proto.Equal(retried, plan) {
			t.Fatalf("retry changed sealed plan: %v", err)
		}
		retryBodies, err := resolveArtifacts(ctx, assignment.Task.Record, retried)
		if err != nil || retryBodies == nil || len(retryBodies.Bodies) != 1 ||
			string(retryBodies.Bodies[0].Body) != "exit 0" {
			t.Fatalf("retry body admission = %v", err)
		}
		clear(retryBodies.Bodies[0].Body)
		checkManualJourneyEntry(t, retryBodies, bindings, entryValue)
		fixture.CompleteManualScript(t, scripts, assignment)
		if withEntry {
			fixture.CheckManualJourneyEntryReference(t, entryRecord, false)
		}
		script, err := scripts.GetScript(ctx, task.Target)
		if err != nil || script.Record.ActiveReferences != 0 {
			t.Fatalf("terminal body fence = %v", err)
		}
		if artifacts, err := resolveArtifacts(ctx, assignment.Task.Record, plan); err == nil ||
			artifacts != nil {
			t.Fatal("completed execution still admitted its body")
		}
		fixture.RemoveCompletedManualScript(t, scripts, task.Target)
		fixture.CheckManualJourneyServiceRemoval(t, intent.ServiceID, false)
		if withEntry {
			fixture.CheckManualJourneyEntryRemoval(t, entryRecord.Entry.ID, false)
		}
		if secretID != "" {
			fixture.CheckManualJourneySecretDeletion(t, secretID, false)
		}
	})
}

func checkManualJourneyEntry(t *testing.T, artifacts *agentpb.ScriptAssignmentArtifacts,
	bindings []*agentpb.ScriptRunnerEntryBinding, value string,
) {
	t.Helper()
	if len(artifacts.Entries) != len(bindings) {
		t.Fatal("private Entry artifacts do not cover sealed bindings")
	}
	for index, entry := range artifacts.Entries {
		if !proto.Equal(entry.Binding, bindings[index]) || string(entry.Value) != value {
			t.Fatal("private Entry artifact substituted binding or value")
		}
		clear(entry.Value)
	}
}

type unexpectedManualJourneyCipher struct{ t *testing.T }

func (cipher unexpectedManualJourneyCipher) Seal(context.Context, []byte) ([]byte, error) {
	cipher.t.Fatal("plain Entry journey unexpectedly encrypted a value")
	return nil, nil
}

func (cipher unexpectedManualJourneyCipher) Open(context.Context, []byte) ([]byte, error) {
	cipher.t.Fatal("plain Entry journey unexpectedly decrypted a value")
	return nil, nil
}
