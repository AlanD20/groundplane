package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ReadCheckpointAcknowledgement reads the exact receipt at the successful
// transaction revision. It does not load current resource state: a subsequent
// checkpoint or Point deletion cannot change this acknowledgement.
func (repository *InputRepository) ReadCheckpointAcknowledgement(ctx context.Context,
	request *agentpb.BackupWorkCheckpointRequest, input *agentpb.BackupWorkInput,
	identity workexecution.Identity, revision int64,
) (*agentpb.BackupWorkCheckpointAck, error) {
	validated, err := executionplan.ValidateBackupWorkCheckpoint(request, input, identity)
	if err != nil {
		return nil, err
	}
	raw, err := repository.readCheckpointReceipt(ctx, validated, revision)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	return checkpointAcknowledgement(raw, validated, input, identity, revision)
}

func (repository *InputRepository) readCheckpointReceipt(ctx context.Context,
	validated *agentpb.BackupWorkCheckpointRequest, revision int64,
) ([]byte, error) {
	if revision <= 0 {
		return nil, conflict("Backup checkpoint commit revision is unavailable")
	}
	key := CheckpointKey(validated.Attempt.WorkItemId, validated.SourceOrdinal, validated.CheckpointSequence)
	read, err := repository.readSettlementKeys(ctx, []string{key}, revision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[0].ModRevision != revision {
		return nil, conflict("Backup checkpoint receipt does not belong to its commit")
	}
	return append([]byte(nil), read.Values[0].Value...), nil
}

func checkpointAcknowledgement(raw []byte, request *agentpb.BackupWorkCheckpointRequest,
	input *agentpb.BackupWorkInput, identity workexecution.Identity, revision int64,
) (*agentpb.BackupWorkCheckpointAck, error) {
	ack, err := checkpointReceiptAcknowledgement(raw, request, input, revision)
	if err != nil {
		return nil, err
	}
	return executionplan.ValidateBackupWorkCheckpointAck(ack, request, input, identity)
}

func checkpointReceiptAcknowledgement(raw []byte, request *agentpb.BackupWorkCheckpointRequest,
	input *agentpb.BackupWorkInput, revision int64,
) (*agentpb.BackupWorkCheckpointAck, error) {
	original, source, err := decodeCheckpointReceipt(raw)
	if err != nil {
		return nil, err
	}
	if !proto.Equal(original, request) {
		return nil, conflict("Backup checkpoint sequence was already used by a different report")
	}
	digest, err := executionplan.BackupWorkCheckpointDigest(request)
	if err != nil {
		return nil, err
	}
	ack := &agentpb.BackupWorkCheckpointAck{
		Attempt:            proto.CloneOf(request.Attempt),
		SourceOrdinal:      request.SourceOrdinal,
		CheckpointSequence: request.CheckpointSequence,
		RequestSha256:      digest,
		Committed: &agentpb.CheckpointFence{DedupeKeyModRevision: revision,
			AuthorityDigest: append([]byte(nil), input.Steps[request.SourceOrdinal].StepDigest...)},
		Source: source,
	}
	setSourceFence(source, proto.CloneOf(ack.Committed))
	return ack, nil
}
