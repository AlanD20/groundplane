package backupruntime

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BackupConsumerHelper interface {
	ExecuteBackupConsumer(
		context.Context,
		*agentpb.BackupConsumerHelperRequest,
	) (*agentpb.BackupConsumerHelperResponse, error)
}

type BackupConsumerHost struct {
	journal   *agentworkjournal.Journal
	helper    BackupConsumerHelper
	identity  workexecution.Identity
	ownership interface{ Check() error }
}

func NewBackupConsumerHost(journal *agentworkjournal.Journal, helper BackupConsumerHelper,
	identity workexecution.Identity, ownership interface{ Check() error },
) (*BackupConsumerHost, error) {
	if journal == nil || helper == nil || ownership == nil || identity.Validate() != nil ||
		identity.Executor != workexecution.Agent {
		return nil, errs.New(errs.KindInternal, "Backup Volume consumer host dependencies are missing")
	}
	return &BackupConsumerHost{journal: journal, helper: helper, identity: identity, ownership: ownership}, nil
}

func (host *BackupConsumerHost) Observe(
	ctx context.Context,
	assignment *agentpb.BackupWorkAssignment,
	ordinal uint32,
	serviceID string,
) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
	return host.run(
		ctx,
		assignment,
		ordinal,
		serviceID,
		agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_UNSPECIFIED,
		true,
	)
}

func (host *BackupConsumerHost) Execute(
	ctx context.Context,
	assignment *agentpb.BackupWorkAssignment,
	ordinal uint32,
	serviceID string,
	operation agentpb.BackupConsumerOperation,
) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
	return host.run(ctx, assignment, ordinal, serviceID, operation, false)
}

func (host *BackupConsumerHost) run(
	ctx context.Context,
	assignment *agentpb.BackupWorkAssignment,
	ordinal uint32,
	serviceID string,
	operation agentpb.BackupConsumerOperation,
	inspection bool,
) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if host == nil {
		return nil, digest, errs.New(errs.KindInternal, "Backup Volume consumer host is unavailable")
	}
	if err := host.ownership.Check(); err != nil {
		return nil, digest, err
	}
	owned, _, err := executionplan.OpenBackupWorkAssignment(assignment, host.identity)
	if err != nil {
		return nil, digest, err
	}
	attempt, saved, err := host.journal.LoadBackup(ctx, owned, host.identity)
	if err != nil {
		return nil, digest, err
	}
	if attempt == nil || attempt.StartedAt == nil || saved == nil || saved.Completion != nil ||
		saved.FailureReason != "" ||
		saved.Recovery != nil ||
		uint64(ordinal) >= uint64(len(saved.Sources)) {
		return nil, digest, errs.New(errs.KindStateConflict, "Backup Volume consumer requires a live durable start")
	}
	request := &agentpb.BackupConsumerHelperRequest{
		Assignment:    owned,
		SourceOrdinal: ordinal,
		ServiceId:     serviceID,
		InspectOnly:   inspection,
		Operation:     operation,
		Start: &agentpb.WorkStartAck{
			Attempt:   proto.CloneOf(owned.Attempt),
			StartedAt: timestamppb.New(*attempt.StartedAt),
		},
	}
	if !inspection {
		request.Checkpoint, err = saved.Sources[ordinal].Report()
		if err != nil {
			return nil, digest, err
		}
		request.Acknowledgement, err = saved.Sources[ordinal].Ack()
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
