package executionplan

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type BackupConsumerHelperInput struct {
	Consumer      BackupConsumerInput
	Inspection    bool
	Operation     agentpb.BackupConsumerOperation
	SourceOrdinal uint32
}

func OpenBackupConsumerHelper(
	ctx context.Context,
	request *agentpb.BackupConsumerHelperRequest,
) (BackupConsumerHelperInput, error) {
	if ctx == nil || request == nil || proto.Size(request) > MaximumPlanBytes || RejectUnknown(request) != nil {
		return BackupConsumerHelperInput{}, invalidBackupConsumerHelper()
	}
	if err := ctx.Err(); err != nil {
		return BackupConsumerHelperInput{}, err
	}
	wire := request.GetAssignment().GetAttempt()
	if (request.Assignment == nil) == (request.Recovery == nil) {
		return BackupConsumerHelperInput{}, invalidBackupConsumerHelper()
	}
	if request.Recovery != nil {
		wire = request.Recovery.Attempt
	}
	identity := workexecution.Identity{
		Executor:   workexecution.Agent,
		HostID:     wire.GetHostId(),
		AgentID:    wire.GetAgentId(),
		Generation: wire.GetExecutorGeneration(),
	}
	var consumer BackupConsumerInput
	var err error
	if request.Recovery != nil {
		consumer, err = OpenBackupRecoveryConsumer(request.Recovery, identity, request.SourceOrdinal, request.ServiceId)
	} else {
		consumer, err = OpenBackupConsumer(request.Assignment, identity, request.SourceOrdinal, request.ServiceId)
	}
	if err != nil {
		return BackupConsumerHelperInput{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(request.Start, consumer.Attempt)
	if err != nil {
		return BackupConsumerHelperInput{}, err
	}
	now := time.Now().UTC()
	if started.After(now) || !now.Before(consumer.Attempt.Deadline) ||
		(request.Recovery == nil && !now.Before(time.Unix(0, int64(consumer.Source.StepDeadlineUnixNano)))) {
		return BackupConsumerHelperInput{}, invalidBackupConsumerHelper()
	}
	consumer.Attempt.StartedAt = &started
	result := BackupConsumerHelperInput{
		Consumer:      consumer,
		Inspection:    request.InspectOnly,
		Operation:     request.Operation,
		SourceOrdinal: request.SourceOrdinal,
	}
	if request.InspectOnly {
		if request.Operation != agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_UNSPECIFIED ||
			request.Checkpoint != nil ||
			request.Acknowledgement != nil {
			return BackupConsumerHelperInput{}, invalidBackupConsumerHelper()
		}
		return result, nil
	}
	if request.Checkpoint.GetSourceOrdinal() != request.SourceOrdinal ||
		!proto.Equal(request.Checkpoint.GetAttempt(), wire) {
		return BackupConsumerHelperInput{}, invalidBackupConsumerHelper()
	}
	var ack *agentpb.BackupWorkCheckpointAck
	if request.Recovery != nil {
		ack, err = ValidateBackupRestoreRecoveryCheckpointAck(
			request.Acknowledgement,
			request.Checkpoint,
			request.Recovery,
			identity,
		)
	} else {
		ack, err = ValidateBackupWorkCheckpointAck(request.Acknowledgement, request.Checkpoint, request.Assignment.Input, identity)
	}
	if err != nil {
		return BackupConsumerHelperInput{}, err
	}
	intent, progress, err := backupConsumerProgress(consumer.Source, request.Checkpoint, ack.Source.GetRestore())
	if err != nil {
		return BackupConsumerHelperInput{}, err
	}
	index := slices.Index(consumer.Source.ConsumerServiceIds, request.ServiceId)
	if index < 0 || intent.serviceID != request.ServiceId || intent.cursor != uint32(index) ||
		progress.cursor != intent.cursor || progress.phase != intent.phase ||
		consumer.Fact.PriorRuntimeIntent.Kind != agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING {
		return BackupConsumerHelperInput{}, invalidBackupConsumerHelper()
	}
	switch request.Operation {
	case agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_STOP:
		if intent.phase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT {
			return BackupConsumerHelperInput{}, invalidBackupConsumerHelper()
		}
	case agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_RECOVER:
		if intent.phase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT {
			return BackupConsumerHelperInput{}, invalidBackupConsumerHelper()
		}
	default:
		return BackupConsumerHelperInput{}, invalidBackupConsumerHelper()
	}
	return result, nil
}

func invalidBackupConsumerHelper() error {
	return errs.New(
		errs.KindStateConflict,
		"Volume consumer command lacks its captured workload and acknowledged execution intent",
	)
}
