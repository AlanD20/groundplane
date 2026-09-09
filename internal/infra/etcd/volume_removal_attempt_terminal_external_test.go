package etcd_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a failed or timed-out attempt is not the end of Volume removal.
// Its root response, locks and exact cursor/pending call must survive unchanged.
func TestVolumeRemovalAttemptTerminalRetainsOperation(t *testing.T) {
	for _, checkpoint := range []string{"published", "detached", "pending", "partial", "absent"} {
		for _, mode := range []string{"failed", "timeout", "stale agent"} {
			t.Run(checkpoint+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				fixture, tasks, assignment, runtime := prepareVolumeRemovalAttemptTerminal(t, checkpoint)
				markerKey, err := etcd.CapabilityIdempotencyMarkerKey(fixture.Marker.Locator)
				if err != nil {
					t.Fatal(err)
				}
				keys := []string{
					removalrecord.RuntimeKey(runtime.OperationID),
					removalrecord.ProgressKey(runtime.OperationID),
					removalrecord.PendingPathKey(runtime.OperationID),
					removalrecord.AttemptKey(runtime.OperationID, 1),
					removalrecord.OwnerKey(
						runtime.VolumeID,
					),
					removalrecord.EnvironmentLockKey(runtime.EnvironmentID),
					markerKey,
				}
				before, err := fixture.Store.GetMany(ctx, etcd.GetManyRequest{Keys: keys})
				if err != nil {
					t.Fatal(err)
				}
				status := etcd.TaskStatusFailed
				terminalAt := runtime.CreatedAt.Add(10 * time.Second)
				result := etcd.TaskResultRecord{
					Kind:       etcd.TaskResultEnvironmentDirectory,
					Diagnostic: etcd.TaskResultDiagnosticNone,
				}
				if mode == "failed" {
					_, err = tasks.AcknowledgeTask(
						ctx,
						assignment.AgentID,
						1,
						assignment.TaskID,
						assignment.AssignmentID,
						status,
						result,
						terminalAt,
					)
				} else {
					status = etcd.TaskStatusTimedOut
					terminalAt = runtime.CreatedAt.Add(time.Second + 6*time.Hour)
					var count int
					if mode == "timeout" {
						count, err = tasks.ExpireTimedOutTasks(ctx, terminalAt)
					} else {
						count, err = tasks.TimeoutAgentAssignments(ctx, assignment.AgentID, 1, 24, terminalAt)
					}
					if err == nil && count != 1 {
						t.Fatalf("timeout count: %d", count)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				fixture.AssertRemovalAttemptBudget(t, status)
				current, err := tasks.GetTask(ctx, assignment.TaskID)
				if err != nil || current.Record.Status != status || current.Record.Result == nil ||
					current.Record.Result.Kind != etcd.TaskResultEnvironmentDirectory {
					t.Fatalf("terminal attempt: %v/%v", current.Record.Status, err)
				}
				after, err := fixture.Store.GetMany(ctx, etcd.GetManyRequest{Keys: keys})
				if err != nil {
					t.Fatal(err)
				}
				for index, value := range before.Values {
					if value == nil {
						if after.Values[index] != nil {
							t.Fatalf("attempt created operation authority %s", keys[index])
						}
						continue
					}
					if after.Values[index] == nil || value.ModRevision != after.Values[index].ModRevision ||
						!bytes.Equal(value.Value, after.Values[index].Value) {
						t.Fatalf("attempt terminalization changed retained authority %s", keys[index])
					}
				}
				marker, err := etcd.DecodeCapabilityIdempotencyMarker(
					after.Values[len(keys)-1].Value,
					fixture.Marker.Locator,
				)
				if err != nil || marker.State != etcd.IdempotencyMarkerPending || !marker.RetainUntil.IsZero() ||
					!marker.TerminalAt.IsZero() {
					t.Fatalf("root response became expirable: %v", err)
				}
				idempotency, err := etcd.NewIdempotencyRepository(fixture.Store)
				if err != nil {
					t.Fatal(err)
				}
				pruneAt := current.Record.RetainUntil.Add(time.Hour)
				// The fixture's initial policy PUT has its own expired marker.
				seedKey, err := etcd.CapabilityIdempotencyMarkerKey(etcd.IdempotencyLocator{
					ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: runtime.EnvironmentID,
					Method: "PUT", Route: "/environments/{id}/backup-policy", Key: "volume-policy-desired-seed-0001",
				})
				if err != nil {
					t.Fatal(err)
				}
				seed, err := fixture.Store.GetMany(ctx, etcd.GetManyRequest{Keys: []string{seedKey}})
				if err != nil || seed.Values[0] == nil {
					t.Fatalf("missing seed marker: %v", err)
				}
				if count, err := idempotency.PruneExpired(ctx, pruneAt); err != nil || count != 1 {
					t.Fatalf("expired seed collection: %d/%v", count, err)
				}
				seed, err = fixture.Store.GetMany(ctx, etcd.GetManyRequest{Keys: []string{seedKey}})
				if err != nil || seed.Values[0] != nil {
					t.Fatalf("expired seed marker survived: %v", err)
				}
				retained, err := fixture.Store.GetMany(ctx, etcd.GetManyRequest{Keys: keys})
				if err != nil {
					t.Fatal(err)
				}
				for index, value := range after.Values {
					if value == nil {
						if retained.Values[index] != nil {
							t.Fatalf("collection created authority %s", keys[index])
						}
						continue
					}
					if retained.Values[index] == nil || value.ModRevision != retained.Values[index].ModRevision ||
						!bytes.Equal(value.Value, retained.Values[index].Value) {
						t.Fatalf("collection changed retained authority %s", keys[index])
					}
				}
				beforeReplay := fixture.Revision()
				if count, err := tasks.PruneExpiredTasks(ctx, pruneAt); err != nil || count != 0 ||
					fixture.Revision() != beforeReplay {
					t.Fatalf("retained attempt was pruned: %d/%v", count, err)
				}
				if _, err := tasks.AcknowledgeTask(ctx, assignment.AgentID, 1, assignment.TaskID, assignment.AssignmentID,
					status, *current.Record.Result, terminalAt); err != nil ||
					fixture.Revision() != beforeReplay {
					t.Fatalf("attempt acknowledgement replay changed state: %v", err)
				}
				replayed, err := fixture.Publish(ctx)
				if err != nil {
					t.Fatal(err)
				}
				outcome, _, conflict, err := replayed.Classify()
				if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownExisting ||
					fixture.Revision() != beforeReplay {
					t.Fatalf("root replay changed: %v/%v/%v", outcome, conflict, err)
				}
			})
		}
	}
}

// Rationale: losing either operation owner must reject the entire attempt
// terminal transaction, even if the owner changes after all preparation reads.
func TestVolumeRemovalAttemptTerminalRejectsChangedOwner(t *testing.T) {
	for _, family := range []string{"volume", "environment"} {
		for _, mode := range []string{"held", "late"} {
			t.Run(family+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				fixture, _, assignment, runtime := prepareVolumeRemovalAttemptTerminal(t, "pending")
				key := removalrecord.OwnerKey(runtime.VolumeID)
				if family == "environment" {
					key = removalrecord.EnvironmentLockKey(runtime.EnvironmentID)
				}
				owner, err := removalrecord.EncodeOwner(removalrecord.Owner{VolumeID: runtime.VolumeID,
					EnvironmentID: runtime.EnvironmentID, OperationID: ids.New(ids.KindOperation)})
				if err != nil {
					t.Fatal(err)
				}
				inject := func() {
					if _, err := fixture.Store.Put(ctx, key, owner); err != nil {
						t.Fatal(err)
					}
				}
				backend := &volumeAttemptTerminalRaceStore{Store: fixture.Store}
				if mode == "held" {
					inject()
				} else {
					backend.before = inject
				}
				tasks, err := etcd.NewTaskRepository(backend)
				if err != nil {
					t.Fatal(err)
				}
				before := fixture.Revision()
				_, err = tasks.AcknowledgeTask(ctx, assignment.AgentID, 1, assignment.TaskID, assignment.AssignmentID,
					etcd.TaskStatusFailed, etcd.TaskResultRecord{Kind: etcd.TaskResultEnvironmentDirectory,
						Diagnostic: etcd.TaskResultDiagnosticNone}, runtime.CreatedAt.Add(10*time.Second))
				writes := int64(0)
				if mode == "late" {
					writes = 1
				}
				if kind, _ := errs.KindOf(err); kind != errs.KindStateConflict || fixture.Revision() != before+writes {
					t.Fatalf("attempt ignored changed owner: %v; revision delta %d", err, fixture.Revision()-before)
				}
				current, err := tasks.GetTask(ctx, assignment.TaskID)
				if err != nil || current.Record.Status != etcd.TaskStatusRunning {
					t.Fatal("losing attempt terminalized Task", err)
				}
			})
		}
	}
}

type volumeAttemptTerminalRaceStore struct {
	etcd.Store
	before func()
}

func (store *volumeAttemptTerminalRaceStore) Transact(ctx context.Context, conditions []etcd.Condition,
	mutations []etcd.Mutation) (etcd.TransactionResult, error) {
	if store.before != nil {
		before := store.before
		store.before = nil
		before()
	}
	return store.Store.Transact(ctx, conditions, mutations)
}

func (*volumeAttemptTerminalRaceStore) ValidateBlueprintTaskTerminal(
	context.Context,
	etcd.BlueprintTaskTerminalTransaction,
) error {
	return errs.New(errs.KindInternal, "Blueprint terminalization is outside the Volume race fixture")
}

func (*volumeAttemptTerminalRaceStore) TransactBlueprintTaskTerminal(
	context.Context,
	etcd.BlueprintTaskTerminalTransaction,
) (etcd.TransactionResult, error) {
	return etcd.TransactionResult{}, errs.New(
		errs.KindInternal,
		"Blueprint terminalization is outside the Volume race fixture",
	)
}

func prepareVolumeRemovalAttemptTerminal(t *testing.T, checkpoint string) (*etcd.VolumePolicyDesiredFixture,
	*etcd.TaskRepository, volumeremoval.EnvironmentVolumeRemovalAssignment, removalrecord.Runtime) {
	t.Helper()
	ctx := context.Background()
	fixture := etcd.NewVolumePolicyDesiredFixture(t)
	runtime := fixture.PrepareRemovalRecords(t)
	stageVolumePolicyDesired(t, fixture)
	published, err := fixture.Publish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := published.Classify()
	if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownApplied {
		t.Fatal("publication failed", err, conflict)
	}
	tasks := fixture.TaskRepository(t)
	agentID := ids.New(ids.KindAgent)
	claimed, found, err := tasks.ClaimNextTask(ctx, agentID, 1, fixture.Task.CreatedAt.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("claim: %v/%v", found, err)
	}
	assignment := volumeremoval.EnvironmentVolumeRemovalAssignment{
		OperationID:     runtime.OperationID,
		TaskID:          fixture.Task.ID,
		AssignmentID:    claimed.Assignment.Record.AssignmentID,
		AgentID:         agentID,
		AgentGeneration: 1,
	}
	if checkpoint == "published" {
		return fixture, tasks, assignment, runtime
	}
	removal, err := volumeremoval.NewEnvironmentVolumeRemovalRuntimeRepository(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := removal.MarkConsumersDetached(ctx, assignment, runtime.CreatedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if checkpoint == "detached" {
		return fixture, tasks, assignment, runtime
	}
	pending, _, err := removal.BeginPathCall(ctx, assignment, runtime.CreatedAt.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint == "pending" {
		return fixture, tasks, assignment, runtime
	}
	completion := removalrecord.Completion{
		OperationID:     runtime.OperationID,
		RequestOrdinal:  pending.Record.RequestOrdinal,
		RequestSHA256:   pending.Record.RequestSHA256,
		DirectoryAbsent: checkpoint == "absent",
		MutationCount:   1,
		CompletedAt:     runtime.CreatedAt.Add(4 * time.Second),
	}
	if checkpoint == "partial" {
		completion.NextCursor, completion.NextComponentStack = []byte("retained-cursor"), []string{"nested"}
	}
	completion.ResponseBytes = removalrecord.PathResponseBytes(completion)
	completion.ResponseSHA256 = removalrecord.PathResponseDigest(completion)
	if _, _, err := removal.CompletePathCall(ctx, volumeremoval.EnvironmentVolumeRemovalPathResult{
		Assignment: assignment, RequestOrdinal: completion.RequestOrdinal, RequestSHA256: completion.RequestSHA256,
		ResponseSHA256: completion.ResponseSHA256, ResponseBytes: completion.ResponseBytes, MutationCount: completion.MutationCount,
		DirectoryAbsent: completion.DirectoryAbsent, NextCursor: completion.NextCursor, NextComponentStack: completion.NextComponentStack,
		CompletedAt: completion.CompletedAt,
	}); err != nil {
		t.Fatal(err)
	}
	return fixture, tasks, assignment, runtime
}
