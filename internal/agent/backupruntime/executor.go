package backupruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Exchange interface {
	BackupRecoveryCheckpoint(context.Context, *agentpb.BackupRecoveryAssignment, *agentpb.BackupWorkCheckpointRequest) (*agentpb.BackupWorkCheckpointAck, error)
	PrepareConfigRecovery(context.Context, *agentpb.BackupRecoveryAssignment, uint32) (*agentpb.BackupConfigRecoveryPrepareAck, error)
	RegisterBackupConfigs(*agentpb.BackupWorkAssignment) error
	ReleaseBackupConfigs(string)
	CaptureConfigStream(backupconfigtransfer.Binding) (*backupconfiguration.CaptureStream, error)
	RestoreConfigStream(context.Context, backupconfigtransfer.Binding) (*backupconfiguration.RestoreStream, error)
	SourceExchange
	VolumeExchange
	StartAttempt(context.Context, workexecution.Attempt) (*agentpb.WorkStartAck, error)
	CompleteBackup(context.Context, *agentpb.BackupWorkCompletionRequest) (*agentpb.BackupWorkCompletionAck, error)
	FailBackup(context.Context, *agentpb.BackupWorkFailureRequest) (*agentpb.BackupWorkFailureAck, error)
	RecoverBackup(context.Context, *agentpb.BackupRecoveryRequest) (*agentpb.BackupRecoveryAck, error)
	RegisterBackupSecrets(*agentpb.BackupWorkAssignment) error
	RegisterBackupRecoverySecrets(*agentpb.BackupRecoveryAssignment) error
	ReleaseBackupSecrets(string)
	RegisterBackupVolumes(*agentpb.BackupWorkAssignment) error
	ReleaseBackupVolumes(string)
}

type Executor struct {
	journal      *agentworkjournal.Journal
	volumeRoot   string
	helper       BackupConsumerHelper
	materializer backupconfiguration.ConfigFileMaterializer
	ownership    interface{ Check() error }
}

func NewExecutor(journal *agentworkjournal.Journal, volumeRoot string, helper BackupConsumerHelper, materializer backupconfiguration.ConfigFileMaterializer, ownership interface{ Check() error }) (*Executor, error) {
	if journal == nil || volumeRoot == "" || helper == nil || materializer == nil || ownership == nil {
		return nil, errs.New(errs.KindInternal, "Backup execution requires a durable Agent journal")
	}
	return &Executor{journal: journal, volumeRoot: volumeRoot, helper: helper, materializer: materializer, ownership: ownership}, nil
}

func (executor *Executor) Run(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, exchange Exchange, staging *Staging,
) (workexecution.Attempt, error) {
	if executor == nil || ctx == nil || exchange == nil {
		return workexecution.Attempt{}, errs.New(errs.KindInternal, "Backup execution is not configured")
	}
	owned, attempt, err := executionplan.OpenBackupWorkAssignment(assignment, identity)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	savedAttempt, saved, err := executor.journal.LoadBackup(ctx, owned, identity)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved != nil && saved.Completion != nil {
		return *saved.Completion, nil
	}
	if saved != nil && saved.FailureReason != "" {
		delivery, err := checkpointdelivery.NewBackup(owned, *savedAttempt, executor.journal, exchange.BackupCheckpoint)
		if err != nil {
			return workexecution.Attempt{}, err
		}
		return delivery.Fail(ctx, saved.FailureReason, exchange.FailBackup)
	}
	if err := executor.requireSources(owned, identity, staging); err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.StartAttempt(ctx, attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	started, reconcile, err := workexecution.DecodeAgentStartAck(ack, attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	attempt.StartedAt = &started
	if savedAttempt != nil && (savedAttempt.StartedAt == nil || !savedAttempt.StartedAt.Equal(started)) {
		return workexecution.Attempt{}, errs.New(errs.KindStateConflict, "Backup start grant differs from its local journal")
	}
	if reconcile && saved == nil {
		// No local receipt is not a clean first execution. Stop this attempt;
		// incomplete exact deletions remain resource-owned pending work.
		delivery, err := checkpointdelivery.NewBackup(owned, attempt, executor.journal, exchange.BackupCheckpoint)
		if err != nil {
			return workexecution.Attempt{}, err
		}
		return delivery.Fail(ctx, executionplan.BackupExecutionInterrupted, exchange.FailBackup)
	}
	if err := executor.journal.StartBackup(ctx, owned, attempt, time.Now().UTC()); err != nil {
		return workexecution.Attempt{}, err
	}
	if err := exchange.RegisterBackupSecrets(owned); err != nil {
		return workexecution.Attempt{}, err
	}
	defer exchange.ReleaseBackupSecrets(attempt.ID)
	if err := executor.runSources(ctx, owned, attempt, exchange, staging); err != nil {
		// Source execution has returned. Only report delivery may outlive its
		// deadline; this context cannot run another remote deletion.
		reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		delivery, deliveryErr := checkpointdelivery.NewBackup(owned, attempt, executor.journal, exchange.BackupCheckpoint)
		if deliveryErr != nil {
			return workexecution.Attempt{}, deliveryErr
		}
		reason := executionplan.BackupExecutionFailed
		if !time.Now().Before(attempt.Deadline) {
			reason = executionplan.BackupDeadlineExpired
		}
		return delivery.Fail(reportCtx, reason, exchange.FailBackup)
	}
	delivery, err := checkpointdelivery.NewBackup(owned, attempt, executor.journal, exchange.BackupCheckpoint)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return delivery.Complete(ctx, exchange.CompleteBackup)
}

func (executor *Executor) Retire(ctx context.Context, terminal workexecution.Attempt) error {
	return executor.journal.Retire(ctx, terminal)
}
