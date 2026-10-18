package backupwork

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: an old checkpoint ACK must carry its exact saved source cursor,
// never a later mutable cursor or a changed MySQL observation at the same sequence.
func TestCheckpointReceiptPinsMySQLObservationAndRejectsChangedReplay(t *testing.T) {
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	digest := bytes.Repeat([]byte{1}, 32)
	attempt, err := workexecution.EncodeAgentAttempt(workexecution.Attempt{
		ID: ids.NewAt(ids.KindAttempt, at, 1), TaskID: ids.NewAt(ids.KindTask, at, 2),
		WorkItemID: ids.NewAt(ids.KindWorkItem, at, 3), Number: 1, Direction: workexecution.Forward,
		Input: workexecution.Input{ID: ids.NewAt(ids.KindPlan, at, 4), SHA256: strings.Repeat("a", 64)},
		Executor: workexecution.Identity{Executor: workexecution.Agent, HostID: ids.NewAt(ids.KindHost, at, 5),
			AgentID: ids.NewAt(ids.KindAgent, at, 6), Generation: 1},
		AssignedAt: at, Deadline: at.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := &agentpb.BackupWorkCheckpointRequest{
		Attempt: attempt, CheckpointSequence: 1,
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_MysqlContainerObserved{
			MysqlContainerObserved: &agentpb.BackupMySQLContainerObserved{
				ServiceId: ids.NewAt(ids.KindService, at, 7), ContainerId: strings.Repeat("a", 64),
				ImageReferenceSha256: digest, ObservedLabelCount: 5, ObservedLabelsSha256: digest,
				ObservationSha256: digest, DatabaseImageIdSha256: digest,
			},
		},
	}
	source := &agentpb.BackupStepResume{
		StepId: ids.NewAt(ids.KindStep, at, 8), ExecutionId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Operation: &agentpb.BackupStepResume_Capture{Capture: &agentpb.BackupCaptureResume{
			CheckpointSequence: 1,
			Checkpoint: &agentpb.BackupCaptureResume_MysqlContainerObserved{
				MysqlContainerObserved: proto.CloneOf(request.GetMysqlContainerObserved()),
			},
		}},
	}
	raw, err := encodeCheckpointReceipt(request, source)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(raw)
	source.GetCapture().CheckpointSequence = 9
	source.GetCapture().GetMysqlContainerObserved().ContainerId = strings.Repeat("c", 64)
	input := &agentpb.BackupWorkInput{Steps: []*agentpb.BackupStepAuthority{{StepDigest: digest}}}
	ack, err := checkpointReceiptAcknowledgement(raw, request, input, 17)
	if err != nil || ack.GetSource().GetCapture().GetCheckpointSequence() != 1 ||
		ack.GetSource().GetCapture().GetMysqlContainerObserved().GetContainerId() != strings.Repeat("a", 64) ||
		ack.GetCommitted().GetDedupeKeyModRevision() != 17 {
		t.Fatalf("saved checkpoint changed with caller cursor: %v", err)
	}
	request.GetMysqlContainerObserved().ContainerId = strings.Repeat("b", 64)
	if _, err := checkpointReceiptAcknowledgement(raw, request, input, 17); err == nil {
		t.Fatal("changed observation accepted at the original checkpoint sequence")
	}
}
