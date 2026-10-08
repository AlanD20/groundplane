package backupruntime

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BAK-03: a valid MySQL observation must be durably acknowledged before Dump;
// retained evidence must still reject a changed payload during replay.
func TestMySQLCheckpointRetainsAuthenticatedObservation(t *testing.T) {
	digest := bytes.Repeat([]byte{1}, 32)
	request := &agentpb.BackupCheckpointRequest{
		TaskId: testBackupTaskID, AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", ExecutionId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		CheckpointSequence: 1, AuthorityDigest: digest,
		Checkpoint: &agentpb.BackupCheckpointRequest_MysqlContainerObserved{
			MysqlContainerObserved: &agentpb.BackupMySQLContainerObserved{
				ServiceId: testBackupServiceID, ContainerId: strings.Repeat("a", 64),
				ImageReferenceSha256: digest, ObservedLabelCount: 5, ObservedLabelsSha256: digest,
				ObservationSha256: digest, DatabaseImageIdSha256: digest,
			},
		},
	}
	input := BackupCheckpointInput{TaskID: request.TaskId, AssignmentID: request.AssignmentId,
		StepID: request.StepId, ExecutionID: request.ExecutionId, Sequence: 1,
		AssignmentGeneration: 1, AuthoritySHA256: hex.EncodeToString(digest), Request: request}
	payload, err := BackupCheckpointDigest(input)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	record := BackupCheckpointDedupRecord{TaskID: input.TaskID, AssignmentID: input.AssignmentID,
		StepID: input.StepID, ExecutionID: input.ExecutionID, Sequence: 1, AssignmentGeneration: 1,
		AuthoritySHA256: input.AuthoritySHA256, CheckpointTag: BackupCheckpointTag(request),
		PayloadSHA256: payload, Request: encoded}
	stored, err := EncodeBackupCheckpointDedupRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := DecodeBackupCheckpointDedupRecord(stored)
	if err != nil || !bytes.Equal(replayed.Request, encoded) {
		t.Fatalf("retained MySQL observation differs: %v", err)
	}
	request.GetMysqlContainerObserved().ContainerId = strings.Repeat("b", 64)
	record.Request, err = proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeBackupCheckpointDedupRecord(record); err == nil {
		t.Fatal("changed observation accepted under the original checkpoint digest")
	}
}
