package app

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskcheckpoint "github.com/AlanD20/groundplane/internal/controller/taskcheckpoint"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: the live checkpoint service must bind the real DELETE assignment,
// save every call before acknowledgement, and replay a lost response read-only.
func TestVolumeRemovalProductionCheckpointService(t *testing.T) {
	fixture, mutations, reads, created := newVolumeRemovalProductionJourney(t)
	ctx := context.Background()
	impact, err := reads.GetVolumeDeletionImpact(ctx, created.Volume.ID, "", 40)
	if err != nil {
		t.Fatal(err)
	}
	response, err := mutations.RemoveVolume(
		ctx,
		created.Volume.ID,
		impact.ImpactToken,
		created.Volume.Key,
		"018f3111-0000-7000-8000-000000000006",
	)
	if err != nil {
		t.Fatal(err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatal(err)
	}
	agentID := ids.New(ids.KindAgent)
	claimed, found, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, time.Now().UTC())
	if err != nil || !found || claimed.Task.Record.ID != accepted.TaskID {
		t.Fatalf("claim real DELETE: %v", err)
	}
	task := claimed.Task.Record
	runtime, err := volumeremoval.NewEnvironmentVolumeRemovalRuntimeRepository(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	service, err := testtaskcheckpoint.NewVolumeRemovalCheckpointService(runtime)
	if err != nil {
		t.Fatal(err)
	}
	planHash, err := hex.DecodeString(task.PlanHash)
	if err != nil {
		t.Fatal(err)
	}
	request := &agentpb.VolumeRemovalCheckpointRequest{
		RequestId:         ids.NewULID(),
		TaskId:            task.ID,
		OperationId:       task.OperationID,
		AssignmentId:      claimed.Assignment.Record.AssignmentID,
		StepId:            task.Steps[len(task.Steps)-1].ID,
		PlanHash:          planHash,
		ConsumersDetached: true,
	}
	ack, err := service.CheckpointVolumeRemoval(ctx, agentID, 1, request)
	if err != nil {
		t.Fatal(err)
	}
	for ordinal := uint64(1); ordinal <= 2; ordinal++ {
		pending, err := removal.DecodePendingPath(ack.PendingPath)
		if err != nil || pending.RequestOrdinal != ordinal {
			t.Fatalf("pending call %d: %v", ordinal, err)
		}
		stored, err := fixture.Store.Get(ctx, removal.PendingPathKey(task.OperationID))
		if err != nil || stored.Entry == nil || !bytes.Equal(stored.Entry.Value, ack.PendingPath) {
			t.Fatalf("ack preceded durable call: %v", err)
		}
		completion := removal.Completion{
			OperationID:     task.OperationID,
			RequestOrdinal:  ordinal,
			RequestSHA256:   pending.RequestSHA256,
			MutationCount:   128,
			NextCursor:      []byte("next"),
			CompletedAt:     time.Now().UTC(),
			DirectoryAbsent: ordinal == 2,
		}
		if completion.DirectoryAbsent {
			completion.NextCursor = nil
		}
		completion.ResponseSHA256, completion.ResponseBytes = removal.PathResponseDigest(
			completion,
		), removal.PathResponseBytes(
			completion,
		)
		request.Completion, err = removal.EncodeCompletion(completion)
		if err != nil {
			t.Fatal(err)
		}
		request.ConsumersDetached, request.RequestId = false, ids.NewULID()
		ack, err = service.CheckpointVolumeRemoval(ctx, agentID, 1, request)
		if err != nil {
			t.Fatalf("complete call %d: %v", ordinal, err)
		}
		before, err := fixture.Store.Get(ctx, removal.ProgressKey(task.OperationID))
		if err != nil {
			t.Fatal(err)
		}
		replayed, err := service.CheckpointVolumeRemoval(ctx, agentID, 1, request)
		if err != nil || replayed.DirectoryAbsent != ack.DirectoryAbsent ||
			!bytes.Equal(replayed.PendingPath, ack.PendingPath) {
			t.Fatalf("lost checkpoint acknowledgement replay: %v", err)
		}
		after, err := fixture.Store.Get(ctx, removal.ProgressKey(task.OperationID))
		if err != nil || before.Entry.ModRevision != after.Entry.ModRevision {
			t.Fatalf("checkpoint replay wrote progress: %v", err)
		}
	}
	if !ack.DirectoryAbsent {
		t.Fatal("final checkpoint did not prove directory absence")
	}
	_, err = fixture.Tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		claimed.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		testtaskjournal.TaskResultRecord{
			Kind:       testtaskjournal.TaskResultEnvironmentDirectory,
			Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
		},
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("complete real DELETE after checkpoints: %v", err)
	}
	owner, err := fixture.Store.Get(ctx, removal.OwnerKey(task.Target))
	if err != nil || owner.Entry != nil {
		t.Fatalf("completed DELETE retained ownership: %v", err)
	}
}
