package etcd_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a successful Agent report alone cannot prove directory absence.
// The actual Task acknowledgement must retain the published removal authority
// until the Controller has committed its matching path-completion checkpoint.
func TestVolumeRemovalTerminalRejectsUnprovedDirectoryAbsence(t *testing.T) {
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
		t.Fatalf("publish: %v/%v/%v", outcome, conflict, err)
	}
	tasks := fixture.TaskRepository(t)
	agentID := ids.New(ids.KindAgent)
	assignment, found, err := tasks.ClaimNextTask(ctx, agentID, 1, fixture.Task.CreatedAt.Add(time.Second))
	if err != nil || !found || assignment.Task.Record.ID != fixture.Task.ID {
		t.Fatalf("claim removal: %v/%v", found, err)
	}
	before := fixture.Revision()
	_, err = tasks.AcknowledgeTask(ctx, agentID, 1, fixture.Task.ID, assignment.Assignment.Record.AssignmentID,
		etcd.TaskStatusCompleted, etcd.TaskResultRecord{
			Kind: etcd.TaskResultEnvironmentDirectory, Diagnostic: etcd.TaskResultDiagnosticNone,
		}, fixture.Task.CreatedAt.Add(2*time.Second))
	if kind, _ := errs.KindOf(err); kind != errs.KindStateConflict || fixture.Revision() != before {
		t.Fatalf(
			"unproved directory absence terminalized removal: %v; revision delta %d",
			err,
			fixture.Revision()-before,
		)
	}
	for _, key := range []string{removalrecord.RuntimeKey(runtime.OperationID),
		removalrecord.OwnerKey(runtime.VolumeID), removalrecord.EnvironmentLockKey(runtime.EnvironmentID)} {
		read, err := fixture.Store.Get(ctx, key)
		if err != nil || read.Entry == nil {
			t.Fatalf("removal authority lost %s: %v", key, err)
		}
	}
}

// Rationale: after the durable path completion, one acknowledgement must remove
// the operation and both locks at the Task/root-marker terminal revision.
func TestVolumeRemovalTerminalReleasesOwnershipAtomically(t *testing.T) {
	for _, mode := range []string{"normal", "lost response", "held owner", "late owner", "held lock", "late lock",
		"late runtime", "late progress", "late completion", "late pending", "late attempt"} {
		t.Run(mode, func(t *testing.T) {
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
				OperationID: runtime.OperationID, TaskID: fixture.Task.ID,
				AssignmentID: claimed.Assignment.Record.AssignmentID, AgentID: agentID, AgentGeneration: 1,
			}
			removal, err := volumeremoval.NewEnvironmentVolumeRemovalRuntimeRepository(fixture.Store)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := removal.MarkConsumersDetached(ctx, assignment, runtime.CreatedAt.Add(2*time.Second)); err != nil {
				t.Fatal(err)
			}
			pending, _, err := removal.BeginPathCall(ctx, assignment, runtime.CreatedAt.Add(3*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			completion := removalrecord.Completion{
				OperationID:     runtime.OperationID,
				RequestOrdinal:  pending.Record.RequestOrdinal,
				RequestSHA256:   pending.Record.RequestSHA256,
				DirectoryAbsent: true,
				MutationCount:   1,
				CompletedAt:     runtime.CreatedAt.Add(4 * time.Second),
			}
			completion.ResponseBytes = removalrecord.PathResponseBytes(completion)
			completion.ResponseSHA256 = removalrecord.PathResponseDigest(completion)
			if _, _, err := removal.CompletePathCall(ctx, volumeremoval.EnvironmentVolumeRemovalPathResult{
				Assignment: assignment, RequestOrdinal: completion.RequestOrdinal, RequestSHA256: completion.RequestSHA256,
				ResponseSHA256: completion.ResponseSHA256, ResponseBytes: completion.ResponseBytes, MutationCount: completion.MutationCount,
				DirectoryAbsent: true, CompletedAt: completion.CompletedAt,
			}); err != nil {
				t.Fatal(err)
			}
			terminalAt := runtime.CreatedAt.Add(5 * time.Second)
			result := etcd.TaskResultRecord{
				Kind:       etcd.TaskResultEnvironmentDirectory,
				Diagnostic: etcd.TaskResultDiagnosticNone,
			}
			changedKey := ""
			if strings.HasPrefix(mode, "held ") || strings.HasPrefix(mode, "late ") {
				keys := map[string]string{
					"owner":      removalrecord.OwnerKey(runtime.VolumeID),
					"lock":       removalrecord.EnvironmentLockKey(runtime.EnvironmentID),
					"runtime":    removalrecord.RuntimeKey(runtime.OperationID),
					"progress":   removalrecord.ProgressKey(runtime.OperationID),
					"completion": removalrecord.CompletionKey(runtime.OperationID, completion.RequestOrdinal),
					"pending":    removalrecord.PendingPathKey(runtime.OperationID),
					"attempt":    removalrecord.AttemptKey(runtime.OperationID, 1),
				}
				_, target, _ := strings.Cut(mode, " ")
				changedKey = keys[target]
				changedValue := []byte("changed authority")
				if target == "owner" || target == "lock" {
					changedValue, err = removalrecord.EncodeOwner(removalrecord.Owner{
						VolumeID: runtime.VolumeID, EnvironmentID: runtime.EnvironmentID,
						OperationID: ids.New(ids.KindOperation),
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				inject := func() {
					if _, err := fixture.Store.Put(ctx, changedKey, changedValue); err != nil {
						t.Fatal(err)
					}
				}
				if strings.HasPrefix(mode, "held ") {
					inject()
				} else {
					fixture.BeforeRemovalTerminalCommit(inject)
				}
			}
			beforeTerminal := fixture.Revision()
			if mode == "lost response" {
				fixture.LoseRemovalTerminalResponse()
			}
			terminal, err := tasks.AcknowledgeTask(ctx, agentID, 1, fixture.Task.ID, assignment.AssignmentID,
				etcd.TaskStatusCompleted, result, terminalAt)
			if changedKey != "" {
				writes := int64(0)
				if strings.HasPrefix(mode, "late ") {
					writes = 1
				}
				if kind, _ := errs.KindOf(err); kind != errs.KindStateConflict ||
					fixture.Revision() != beforeTerminal+writes {
					t.Fatalf(
						"changed authority terminalized removal: %v; revision delta %d",
						err,
						fixture.Revision()-beforeTerminal,
					)
				}
				stored, err := fixture.Store.Get(ctx, etcd.CapabilityTaskKey(fixture.Task.ID))
				if err != nil || stored.Entry == nil {
					t.Fatal("Task lost", err)
				}
				stillRunning, err := etcd.DecodeCapabilityTaskRecord(stored.Entry.Value)
				if err != nil || stillRunning.Status != etcd.TaskStatusRunning {
					t.Fatal("Task was terminalized", err)
				}
				return
			}
			if mode == "lost response" {
				if kind, _ := errs.KindOf(err); kind != errs.KindRequestFailed {
					t.Fatalf("lost response: %v", err)
				}
				terminal, err = tasks.AcknowledgeTask(ctx, agentID, 1, fixture.Task.ID, assignment.AssignmentID,
					etcd.TaskStatusCompleted, result, terminalAt)
			}
			if err != nil {
				t.Fatal(err)
			}
			if terminal.Revision != beforeTerminal+1 || fixture.Revision() != terminal.Revision {
				t.Fatal("terminalization was not one transaction")
			}
			fixture.AssertRemovalTerminal(t, terminal.Revision, terminalAt)
			fixture.AssertRemovalAncestry(t, beforeTerminal)
			retained, err := fixture.Store.Range(
				ctx,
				etcd.RangeRequest{Prefix: removalrecord.Root(runtime.OperationID), Limit: 1},
			)
			if err != nil || retained == nil || len(retained.Values) != 0 {
				t.Fatalf("finalized operation retained current records: %v", err)
			}
			for _, key := range []string{removalrecord.RuntimeKey(runtime.OperationID), removalrecord.ProgressKey(runtime.OperationID),
				removalrecord.OwnerKey(runtime.VolumeID), removalrecord.EnvironmentLockKey(runtime.EnvironmentID)} {
				read, err := fixture.Store.Get(ctx, key)
				if err != nil || read.Entry != nil {
					t.Fatalf("successful removal retained %s: %v", key, err)
				}
			}
			markerKey, err := etcd.CapabilityIdempotencyMarkerKey(fixture.Marker.Locator)
			if err != nil {
				t.Fatal(err)
			}
			read, err := fixture.Store.Get(ctx, markerKey)
			if err != nil || read.Entry == nil {
				t.Fatal("terminal marker absent", err)
			}
			marker, err := etcd.DecodeCapabilityIdempotencyMarker(read.Entry.Value, fixture.Marker.Locator)
			if err != nil || marker.State != etcd.IdempotencyMarkerCompleted ||
				read.Entry.ModRevision != terminal.Revision ||
				!marker.TerminalAt.Equal(terminalAt) ||
				!marker.RetainUntil.After(terminalAt) ||
				string(marker.Response.Body) != string(fixture.Marker.Response.Body) {
				t.Fatalf("terminal marker not atomic: %v", err)
			}
			before := fixture.Revision()
			if _, err := tasks.AcknowledgeTask(ctx, agentID, 1, fixture.Task.ID, assignment.AssignmentID,
				etcd.TaskStatusCompleted, result, terminalAt); err != nil || fixture.Revision() != before {
				t.Fatalf("terminal acknowledgement replay changed state: %v", err)
			}
			replayed, err := fixture.Publish(ctx)
			if err != nil {
				t.Fatal(err)
			}
			outcome, _, conflict, err = replayed.Classify()
			if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownExisting ||
				fixture.Revision() != before {
				t.Fatalf("root DELETE response did not survive finalization: %v/%v/%v", outcome, conflict, err)
			}
		})
	}
}
