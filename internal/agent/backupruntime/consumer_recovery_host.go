package backupruntime

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (host *BackupConsumerHost) RunRecovery(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	ordinal uint32, serviceID string, operation agentpb.BackupConsumerOperation, inspection bool,
) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if host == nil {
		return nil, digest, errs.New(errs.KindInternal, "Restore consumer recovery host is unavailable")
	}
	if err := host.ownership.Check(); err != nil {
		return nil, digest, err
	}
	owned, _, _, err := executionplan.OpenBackupRecoveryAssignment(assignment, host.identity)
	if err != nil {
		return nil, digest, err
	}
	attempt, saved, err := host.journal.LoadBackupRecovery(ctx, owned, host.identity)
	if err != nil {
		return nil, digest, err
	}
	if attempt == nil || attempt.StartedAt == nil || saved == nil || saved.Recovery == nil ||
		saved.Completion != nil || saved.FailureReason != "" || len(saved.Recovery.Report) != 0 || ordinal != 0 {
		return nil, digest, errs.New(
			errs.KindStateConflict,
			"Restore consumer recovery requires its active durable start",
		)
	}
	request := &agentpb.BackupConsumerHelperRequest{Recovery: owned, SourceOrdinal: ordinal, ServiceId: serviceID,
		Operation: operation, InspectOnly: inspection,
		Start: &agentpb.WorkStartAck{
			Attempt:   proto.CloneOf(owned.Attempt),
			StartedAt: timestamppb.New(*attempt.StartedAt),
		}}
	if !inspection {
		request.Checkpoint, err = saved.Recovery.Checkpoint.Report()
		if err != nil {
			return nil, digest, err
		}
		request.Acknowledgement, err = saved.Recovery.Checkpoint.Ack()
		if err != nil {
			return nil, digest, err
		}
	}
	captured, err := executionplan.OpenBackupConsumerHelper(ctx, request)
	if err != nil {
		return nil, digest, err
	}
	response, err := host.helper.ExecuteBackupConsumer(ctx, request)
	if err != nil {
		return nil, digest, err
	}
	if err := host.ownership.Check(); err != nil {
		return nil, digest, err
	}
	if err := executionplan.ValidateBackupConsumerHelperResponse(captured, response); err != nil {
		return nil, digest, err
	}
	copy(digest[:], response.ObservationSha256)
	return proto.CloneOf(response.Observed), digest, nil
}
