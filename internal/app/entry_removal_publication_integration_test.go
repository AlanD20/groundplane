package app

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprintplanning "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	desiredstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	migratedentryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleasegroups "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: the actual desired publisher must defer Entry visibility changes
// until the real Agent worker succeeds. The helper decodes the real transfer;
// filesystem effects remain faked in this hermetic publication journey.
func TestEntryRemovalPublishesHeadOnlyAfterCleanup(t *testing.T) {
	testEntryRemovalPublication(t, true, false)
}

func TestEntryRemovalNeverAppliedFinalizesWithoutHost(t *testing.T) {
	testEntryRemovalPublication(t, false, false)
}

func TestEntryRemovalFailedCleanupRetriesPinnedCandidate(t *testing.T) {
	testEntryRemovalPublication(t, true, true)
}

func TestEntryRemovalRetainedTaskPinsCleanupAfterResponseExpiry(t *testing.T) {
	testEntryRemovalPublication(t, true, true, true)
}

func testEntryRemovalPublication(t *testing.T, applied, failFirst bool, expireResponse ...bool) {
	t.Helper()
	testBlueprintExecutedArtifactConfigured(t, false, false, nil,
		func(fixture *ExecutedArtifactFixture, resolver *testtaskplanning.TaskPlanResolver,
			_ testreleaserender.ReleaseRenderInput, _ domain.Intent, _ *agentpb.ComposeArtifact) {
			ctx := t.Context()
			publishEntry := fixture.PublishAppliedRemovalJourneyEntry
			if !applied {
				publishEntry = func(t *testing.T, value string) (*migratedentryvalues.Repository, *etcd.SecretRepository, testentries.Record) {
					return fixture.PublishManualJourneyEntry(t, value, nil, "")
				}
			}
			values, secrets, entry := publishEntry(t, "test")
			current, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, entry.EnvironmentID)
			if err != nil || !found {
				t.Fatal("published desired Entry is unavailable", err)
			}
			protector, ciphertext := manualJourneyEncryptedValue(t, "intent-test")
			defer clear(ciphertext)
			materials, err := testtaskmaterialization.NewTaskMaterializationResolver(
				fixture.Hierarchy,
				values,
				secrets,
				resolver,
				protector,
			)
			if err != nil {
				t.Fatal(err)
			}
			planner, err := testtaskplanning.NewEntryRemovalPlanner(resolver, materials, fixture.Hierarchy)
			if err != nil {
				t.Fatal(err)
			}
			task, marker := fixture.EntryRemovalTask(t, entry.Entry.ID)
			desired, err := desiredstore.NewRepository(fixture.EntryRemovalStore())
			if err != nil {
				t.Fatal(err)
			}
			claim, err := desired.ClaimEnvironmentBlueprintStage(
				ctx,
				testblueprints.EnvironmentBlueprintStageClaimRequest{
					EnvironmentID: entry.EnvironmentID, CandidateRevisionID: task.ID, CandidateTaskID: task.ID,
					Locator: marker.Locator, Intent: marker.Intent, BaselineHeadRevision: current.Revision,
					SourceKind: testblueprints.EnvironmentBlueprintSourceMutation, RenderGeneration: current.Record.RenderGeneration + 1,
					ProjectionSchema: testblueprints.EnvironmentDesiredProjectionSchema, CreatedAt: task.CreatedAt,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			candidate, _, err := testcomposerender.ProjectEnvironmentEntryMutation(
				current.Record, testcomposerender.EnvironmentEntryArtifactMutation{
					RevisionID: task.ID, ArtifactID: ids.New(ids.KindConfig), PlanID: task.PlanID,
					RenderGeneration: claim.RenderGeneration, Entries: nil,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			task, err = planner.PrepareDesiredEntryRemoval(ctx, task, claim)
			if err != nil {
				t.Fatal(err)
			}
			if !applied &&
				(task.Executor != testtaskjournal.TaskExecutorController || task.TimeoutSeconds != 30 || len(task.Materializations) != 0) {
				t.Fatal("never-applied removal contains host work")
			}
			digest, err := testblueprints.EnvironmentBlueprintDependencyDigest(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := desired.StageEnvironmentBlueprintRevision(ctx, testblueprints.EnvironmentBlueprintStageRequest{
				Claim: claim, Projection: candidate, DependencyDigest: digest,
				Mutation: &testblueprints.EnvironmentDesiredMutationAudit{Entry: &testblueprints.EnvironmentEntryMutationAudit{
					Action: testblueprints.EnvironmentEntryMutationRemove, EntryID: entry.Entry.ID, BaseRevisionID: current.Record.RevisionID,
				}},
			}); err != nil {
				t.Fatal(err)
			}
			publish := func() (etcd.IdempotencyTransactionResult, error) {
				return fixture.Hierarchy.PublishEnvironmentDesiredRevisionWithTask(
					ctx,
					fixture.Project,
					fixture.Environment,
					current.Revision,
					claim,
					testblueprints.EnvironmentDesiredRevisionIdentity{
						EnvironmentID: entry.EnvironmentID,
						RevisionID:    task.ID,
					},
					candidate,
					nil,
					nil,
					nil,
					testreleasegroups.ReleaseGroupBlueprintPreparedMutation{},
					testcomponentplanning.ComponentTaskPreparation{},
					testblueprintplanning.BlueprintAttachTaskPreparation{},
					task,
					marker,
				)
			}
			var competingTask string
			if !applied {
				competingTask = fixture.QueueEntryRemovalCompetingWriter(t)
			}
			result, err := publish()
			if err != nil {
				t.Fatal(err)
			}
			if outcome, _, conflict, err := result.Classify(); err != nil || conflict != nil ||
				outcome != etcd.IdempotencyKnownApplied {
				t.Fatalf("DELETE publication: %v / %v / %v", outcome, conflict, err)
			}
			pending, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, entry.EnvironmentID)
			if err != nil || !found || pending.Revision != current.Revision || len(pending.Record.Entries) != 1 {
				t.Fatalf("DELETE changed visible Entry before cleanup: %v / %v", found, err)
			}
			if applied {
				if failFirst {
					failed := acknowledgeEntryRemoval(t, fixture, task, testtaskjournal.TaskStatusFailed)
					if len(expireResponse) != 0 && expireResponse[0] {
						markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
						if err != nil {
							t.Fatal(err)
						}
						if _, err := fixture.EntryRemovalStore().Delete(ctx, markerKey); err != nil {
							t.Fatal(err)
						}
						if _, err := desired.CleanupEnvironmentBlueprintStaging(ctx,
							time.Now().UTC().Add(2*testblueprints.EnvironmentBlueprintStageExpiry), 128); err != nil {
							t.Fatal(err)
						}
					}
					retained, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, entry.EnvironmentID)
					if err != nil || !found || retained.Revision != current.Revision ||
						len(retained.Record.Entries) != 1 {
						t.Fatal("failed cleanup changed desired Entry state", err)
					}
					retryID, retryMarker := fixture.EntryRemovalRetryMarker(t, failed.Record)
					result, err := fixture.Tasks.RetryTask(
						ctx,
						task.ID,
						retryID,
						testtaskjournal.TaskActorOperator,
						retryMarker,
					)
					if err != nil {
						t.Fatal(err)
					}
					if outcome, _, conflict, err := result.Classify(); err != nil || conflict != nil ||
						outcome != etcd.IdempotencyKnownApplied {
						t.Fatalf("cleanup retry: %v / %v / %v", outcome, conflict, err)
					}
					retry, err := fixture.Tasks.GetTask(ctx, retryID)
					if err != nil {
						t.Fatal(err)
					}
					runEntryRemovalAgent(t, fixture, resolver, materials, retry.Record)
				} else {
					runEntryRemovalAgent(t, fixture, resolver, materials, task)
				}
			} else {
				if _, found, err := fixture.Tasks.ClaimNextTask(ctx, ids.New(ids.KindAgent), 1, time.Now().UTC()); err != nil || found {
					t.Fatalf("never-applied cleanup allowed a competing materialization: %v / %v", found, err)
				}
				claimed, found, err := fixture.Tasks.ClaimNextControllerTask(ctx, task.CreatedAt.Add(time.Second))
				if err != nil || !found || claimed.Task.Record.ID != task.ID {
					t.Fatalf("Controller finalizer claim: %v / %v", found, err)
				}
				if _, err := fixture.Tasks.AcknowledgeControllerTask(ctx, task.ID, testtaskjournal.TaskStatusCompleted, task.CreatedAt.Add(2*time.Second)); err != nil {
					t.Fatal(err)
				}
				if claimed, found, err := fixture.Tasks.ClaimNextTask(ctx, ids.New(ids.KindAgent), 1, time.Now().UTC()); err != nil || !found || claimed.Task.Record.ID != competingTask {
					t.Fatalf("completed cleanup retained its materialization exclusion: %v / %v", found, err)
				}
			}
			completed, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, entry.EnvironmentID)
			if err != nil || !found || completed.Record.RevisionID != task.ID || len(completed.Record.Entries) != 0 {
				t.Fatalf("cleanup did not publish Entry removal: %v / %v", found, err)
			}
			generation, found, err := values.GetPlain(ctx, entry.Entry.ID, entry.CurrentValueGenerationID)
			clear(generation.Content)
			if err != nil || found {
				t.Fatal("completed removal retained its value generation", err)
			}
			if len(expireResponse) != 0 && expireResponse[0] {
				return
			}
			replay, err := publish()
			if err != nil {
				t.Fatal(err)
			}
			if outcome, _, conflict, err := replay.Classify(); err != nil || conflict != nil ||
				outcome != etcd.IdempotencyKnownExisting {
				t.Fatalf("DELETE replay: %v / %v / %v", outcome, conflict, err)
			}
		})
}

func acknowledgeEntryRemoval(
	t *testing.T,
	fixture *ExecutedArtifactFixture,
	task etcd.TaskRecord,
	status testtaskjournal.TaskStatus,
) testkeyvalue.Versioned[etcd.TaskRecord] {
	t.Helper()
	ctx := t.Context()
	agentID := ids.New(ids.KindAgent)
	assigned, found, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second))
	if err != nil || !found || assigned.Task.Record.ID != task.ID {
		t.Fatalf("cleanup assignment: %v / %v", found, err)
	}
	result := testtaskjournal.TaskResultRecord{
		Kind:           testtaskjournal.TaskResultCompose,
		ExecutionEpoch: 1,
		Diagnostic:     testtaskjournal.TaskResultDiagnosticNone,
	}
	if status == testtaskjournal.TaskStatusFailed {
		result.Diagnostic = testtaskjournal.TaskResultDiagnosticComposeFailed
	}
	completed, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, assigned.Assignment.Record.AssignmentID,
		status, result, task.CreatedAt.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, assigned.Assignment.Record.AssignmentID,
		status, result, task.CreatedAt.Add(2*time.Second)); err != nil {
		t.Fatal("cleanup acknowledgement replay", err)
	}
	return completed
}
