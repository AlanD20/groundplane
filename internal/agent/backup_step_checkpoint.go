package agent

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// A worker advances its local cursor only after the exact native receipt. A
// disconnected send is not a new predecessor for the following checkpoint.
type backupStepCheckpoint struct {
	pool        *WorkerPool
	taskID      string
	assignID    string
	step        *agentpb.BackupStepAuthority
	sequence    uint64
	fence       *agentpb.CheckpointFence
	lastRequest *agentpb.BackupCheckpointRequest
}

func (publisher *backupStepCheckpoint) publish(ctx context.Context, payload *agentpb.BackupCheckpointRequest) error {
	request := proto.CloneOf(payload)
	request.TaskId, request.AssignmentId = publisher.taskID, publisher.assignID
	request.StepId, request.ExecutionId = publisher.step.StepId, publisher.step.ExecutionId
	request.AuthorityDigest = append([]byte(nil), publisher.step.StepDigest...)
	request.CheckpointSequence = publisher.sequence + 1
	request.PrecedingCheckpoint = proto.CloneOf(publisher.fence)
	ack, err := publisher.pool.commitBackupCheckpoint(ctx, request)
	if err != nil {
		return err
	}
	publisher.sequence, publisher.fence = request.CheckpointSequence, proto.CloneOf(ack.Committed)
	publisher.lastRequest = proto.CloneOf(request)
	return nil
}
