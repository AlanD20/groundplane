package checkpointdelivery

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// TASK-22: a transport failure must retain the exact report across reopening;
// only its matching acknowledgement can unblock the next side-effect boundary.
func TestScriptCheckpointCrashReplayAndAcknowledgementFence(t *testing.T) {
	ctx := context.Background()
	at := time.Now().UTC()
	attempt := workexecution.Attempt{
		ID: ids.New(ids.KindAttempt), TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem), Number: 1,
		Executor: workexecution.Identity{
			Executor:   workexecution.Agent,
			HostID:     ids.New(ids.KindHost),
			AgentID:    ids.New(ids.KindAgent),
			Generation: 1,
		},
		Direction: workexecution.Forward, Input: workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)},
		AssignedAt: at, StartedAt: &at, Deadline: at.Add(time.Minute),
	}
	draft := &agentpb.ScriptCheckpointRequest{
		TaskId: attempt.TaskID, WorkItemId: attempt.WorkItemID, AttemptId: attempt.ID, OperationId: ids.New(ids.KindOperation),
		ScriptExecutionId: ids.NewULID(), InputId: attempt.Input.ID, InputSha256: bytes.Repeat([]byte{0xaa}, 32),
		StageId: ids.New(ids.KindStep), ExpectedState: agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED,
		State: agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED,
		Evidence: &agentpb.ScriptCheckpointRequest_StartAuthorized{
			StartAuthorized: &agentpb.ScriptStartAuthorizedCheckpoint{},
		},
	}
	path := filepath.Join(t.TempDir(), "journal")
	journal, err := agentworkjournal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := journal.Close(); err != nil {
			t.Error(err)
		}
	})
	lost := errors.New("checkpoint response lost")
	var sent *agentpb.ScriptCheckpointRequest
	delivery, err := NewScript(
		attempt,
		journal,
		func(ctx context.Context, request *agentpb.ScriptCheckpointRequest) (*agentpb.ScriptCheckpointAck, error) {
			retained, err := journal.Script(ctx, attempt.ID)
			if err != nil || retained == nil || retained.Ack != nil || !proto.Equal(retained.Report, request) {
				t.Fatalf("report was sent before durable publication: record=%v err=%v", retained, err)
			}
			sent = proto.CloneOf(request)
			return nil, lost
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Checkpoint(ctx, draft); !errors.Is(err, lost) {
		t.Fatalf("Checkpoint = %v, want lost response", err)
	}
	if sent == nil || sent.Sequence != 1 || draft.Sequence != 0 {
		t.Fatal("journal did not allocate one sequence without mutating the caller's draft")
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = agentworkjournal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ack := scriptAcknowledgement(t, sent)
	for _, mismatch := range []string{"attempt", "work", "sequence", "digest"} {
		t.Run(mismatch, func(t *testing.T) {
			wrong := proto.CloneOf(ack)
			switch mismatch {
			case "attempt":
				wrong.AttemptId = ids.New(ids.KindAttempt)
			case "work":
				wrong.WorkItemId = ids.New(ids.KindWorkItem)
			case "sequence":
				wrong.Sequence++
			case "digest":
				wrong.RequestSha256[0] ^= 1
			}
			if err := journal.AcknowledgeScript(ctx, wrong); err == nil {
				t.Fatal("mismatched acknowledgement advanced the durable report")
			}
			retained, err := journal.Script(ctx, attempt.ID)
			if err != nil || retained == nil || retained.Ack != nil || !proto.Equal(retained.Report, sent) {
				t.Fatalf("rejected acknowledgement changed retained report: %v", err)
			}
		})
	}
	next := proto.CloneOf(draft)
	next.StageId = ids.New(ids.KindStep)
	next.ExpectedState = draft.State
	next.State = agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED
	next.Evidence = &agentpb.ScriptCheckpointRequest_BodyPrepared{BodyPrepared: &agentpb.ScriptBodyPreparedCheckpoint{
		BodySha256: bytes.Repeat([]byte{0xcc}, 32), Uid: 1000, Gid: 1000, Device: 1, Inode: 2, Leaf: "body",
	}}
	if _, err := journal.PrepareScript(ctx, attempt, next, time.Now().UTC()); err == nil {
		t.Fatal("a new checkpoint replaced an unacknowledged side-effect boundary")
	}
	calls := 0
	delivery, err = NewScript(
		attempt,
		journal,
		func(_ context.Context, request *agentpb.ScriptCheckpointRequest) (*agentpb.ScriptCheckpointAck, error) {
			calls++
			if !proto.Equal(request, sent) {
				t.Fatal("restart reconstructed a different report, sequence or timestamp")
			}
			return ack, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = agentworkjournal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err = NewScript(
		attempt,
		journal,
		func(context.Context, *agentpb.ScriptCheckpointRequest) (*agentpb.ScriptCheckpointAck, error) {
			calls++
			return nil, errors.New("acknowledged report was sent again")
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	if err := delivery.Checkpoint(ctx, draft); err != nil {
		t.Fatal(err)
	}
	sequence, err := journal.Sequence(ctx, attempt)
	if err != nil || sequence != 1 || calls != 1 {
		t.Fatalf("replay advanced or resent acknowledged work: sequence=%d calls=%d err=%v", sequence, calls, err)
	}
	wrongHost := attempt
	wrongHost.Executor.HostID = ids.New(ids.KindHost)
	if _, err := journal.Sequence(ctx, wrongHost); err == nil {
		t.Fatal("another host acquired the retained attempt's delivery cursor")
	}
}

func scriptAcknowledgement(t *testing.T, request *agentpb.ScriptCheckpointRequest) *agentpb.ScriptCheckpointAck {
	t.Helper()
	digest, err := executionplan.ComputeScriptCheckpointPayloadDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	return &agentpb.ScriptCheckpointAck{
		TaskId: request.TaskId, WorkItemId: request.WorkItemId, AttemptId: request.AttemptId, OperationId: request.OperationId,
		ScriptExecutionId: request.ScriptExecutionId, Sequence: request.Sequence, StageId: request.StageId, State: request.State,
		RequestSha256: digest, EvidenceId: ids.New(ids.KindPlan), EvidenceSha256: bytes.Repeat([]byte{0xbb}, 32),
	}
}
