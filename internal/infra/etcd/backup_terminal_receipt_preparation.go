package etcd

import (
	"crypto/sha256"
	"encoding/hex"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func backupTerminalTaskEvidence(task TaskRecord) (backupruntime.BackupTerminalTaskEvidence, error) {
	if task.FinishedAt == nil || task.RetainUntil == nil {
		return backupruntime.BackupTerminalTaskEvidence{}, errs.New(
			errs.KindValidationFailed,
			"backup terminal receipt requires terminal Task timestamps",
		)
	}
	resultDigest, err := backupruntime.BackupTerminalResultDigest(task.Result)
	if err != nil {
		return backupruntime.BackupTerminalTaskEvidence{}, err
	}
	taskDigest, err := backupTerminalTaskDigest(task)
	if err != nil {
		return backupruntime.BackupTerminalTaskEvidence{}, err
	}
	evidence := backupruntime.BackupTerminalTaskEvidence{
		TaskID: task.ID, TaskType: task.Type, OperationID: task.OperationID, RetryOf: task.RetryOf,
		Owner: task.Owner, Actor: task.Actor, Executor: task.Executor, Target: task.Target,
		PlanID: task.PlanID, PlanHash: task.PlanHash, Status: task.Status,
		ResultDigest: resultDigest, TerminalAssignment: taskjournal.CloneTaskTerminalAssignment(task.TerminalAssignment),
		CreatedAt: task.CreatedAt, UpdatedAt: task.UpdatedAt,
		FinishedAt: *task.FinishedAt, RetainUntil: *task.RetainUntil, TaskDigest: taskDigest,
	}
	if task.StartedAt != nil {
		startedAt := *task.StartedAt
		evidence.StartedAt = &startedAt
	}
	if err := backupruntime.ValidateBackupTerminalTaskEvidence(evidence); err != nil {
		return backupruntime.BackupTerminalTaskEvidence{}, err
	}
	return evidence, nil
}

func prepareBackupRunTerminalReceipt(
	current etcdstore.Versioned[TaskRecord],
	terminal TaskRecord,
	run backupruntime.BackupRunRecord,
) (backupTerminalReceiptPlan, error) {
	if current.Revision <= 0 || current.Record.Type != taskjournal.TaskBackup || terminal.Type != taskjournal.TaskBackup ||
		current.Record.ID != terminal.ID || !taskjournal.IsTerminalTaskStatus(terminal.Status) ||
		ValidateBackupRunTaskBinding(terminal, run) != nil || !backupruntime.TerminalBackupRunState(run.State) ||
		terminal.FinishedAt == nil || !run.UpdatedAt.Equal(*terminal.FinishedAt) {
		return backupTerminalReceiptPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup terminal receipt binding is invalid",
		)
	}
	wantState, err := backupRunStateForTaskStatus(terminal.Status)
	if err != nil || run.State != wantState {
		return backupTerminalReceiptPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup terminal receipt outcome is invalid",
		)
	}
	evidence, err := backupTerminalTaskEvidence(terminal)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	runValue, err := backupruntime.EncodeBackupRunRecord(run)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	defer clear(runValue)
	domainDigest := sha256.Sum256(append([]byte("groundplane.backup.terminal.run.v1\x00"), runValue...))
	receipt := backupruntime.BackupTerminalReceiptRecord{
		Task: evidence, PriorTaskRevision: current.Revision,
		DomainDigest: hex.EncodeToString(domainDigest[:]),
		Sources:      backupTerminalRunOutcomes(run),
	}
	return prepareBackupTerminalReceiptPlan(receipt)
}

func backupTerminalRunOutcomes(run backupruntime.BackupRunRecord) []backupruntime.BackupTerminalSourceOutcome {
	outcomes := make([]backupruntime.BackupTerminalSourceOutcome, len(run.Sources))
	for index, source := range run.Sources {
		outcomes[index] = backupruntime.BackupTerminalSourceOutcome{
			Ordinal: source.Ordinal, SourceID: source.SourceID, Kind: source.Kind,
			TargetID: source.TargetID, RecoveryPointID: source.RecoveryPointID,
			RecoveryPointCreatedAt: source.RecoveryPointCreatedAt,
			State:                  source.State, Phase: source.Phase, SizeBytes: source.SizeBytes,
			SHA256: source.SHA256, FailureCode: source.FailureCode,
		}
	}
	return outcomes
}

func prepareBackupPruneTerminalReceipt(
	current etcdstore.Versioned[TaskRecord],
	terminal TaskRecord,
	dispatch backupruntime.BackupRecoveryPointPruneDispatchRecord,
	prunes []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
) (backupTerminalReceiptPlan, error) {
	if current.Revision <= 0 || current.Record.Type != taskjournal.TaskBackupPrune ||
		terminal.Type != taskjournal.TaskBackupPrune || !taskjournal.IsTerminalTaskStatus(terminal.Status) ||
		terminal.FinishedAt == nil || ValidateBackupPruneTaskBinding(terminal, dispatch) != nil ||
		len(prunes) != len(dispatch.RecoveryPointIDs) || terminal.ID != current.Record.ID {
		return backupTerminalReceiptPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup prune terminal receipt binding is invalid",
		)
	}
	evidence, err := backupTerminalTaskEvidence(terminal)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	receipt := backupruntime.BackupTerminalReceiptRecord{
		Task: evidence, PriorTaskRevision: current.Revision,
		Points: make([]backupruntime.BackupPruneTerminalPointOutcome, len(prunes)),
	}
	for index, prune := range prunes {
		if prune.Revision <= 0 || prune.Record.Point.ID != dispatch.RecoveryPointIDs[index] ||
			prune.Record.OperationID != dispatch.OperationID || prune.Record.TaskID != dispatch.TaskID ||
			prune.Record.Point.EnvironmentID != dispatch.EnvironmentID ||
			prune.Record.CreatedAt.After(*terminal.FinishedAt) {
			return backupTerminalReceiptPlan{}, errs.New(
				errs.KindValidationFailed,
				"backup prune terminal receipt point binding is invalid",
			)
		}
		outcome := backupruntime.BackupPruneTerminalRetained
		if prune.Record.State == backupruntime.BackupPruneVerifiedAbsent {
			outcome = backupruntime.BackupPruneTerminalRemoved
		} else if prune.Record.State != backupruntime.BackupPruneAssigned || terminal.Status == taskjournal.TaskStatusCompleted {
			return backupTerminalReceiptPlan{}, errs.New(
				errs.KindValidationFailed,
				"backup prune terminal receipt outcome is invalid",
			)
		}
		receipt.Points[index] = backupruntime.BackupPruneTerminalPointOutcome{
			Point: prune.Record.Point, CreatedAt: prune.Record.CreatedAt, Outcome: outcome,
		}
	}
	receipt.DomainDigest, err = backupruntime.BackupTerminalDomainDigest(receipt.Points)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	return prepareBackupTerminalReceiptPlan(receipt)
}

func prepareBackupTerminalReceiptPlan(
	receipt backupruntime.BackupTerminalReceiptRecord,
) (backupTerminalReceiptPlan, error) {
	epochDigest, err := backupruntime.BackupTerminalEnvironmentEpochDigest(receipt.Task.Owner.EnvironmentID)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	// The exact prior epoch ModRevision is carried by the domain plan and is
	// bound by composeBackupTerminalTransaction before persistence.
	receipt.PriorEnvironmentEpochRevision = 1
	receipt.EnvironmentEpochDigest = epochDigest
	receipt.ReceiptDigest, err = backupruntime.BackupTerminalReceiptDigest(receipt)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	value, err := backupruntime.EncodeBackupTerminalReceiptRecord(receipt)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	key := backupruntime.BackupTerminalReceiptKey(receipt.Task.TaskID)
	// The terminal Task ModRevision compare serializes every valid receipt
	// writer. A second receipt compare would spend the closed prune transaction's
	// final operation without strengthening that fence.
	return backupTerminalReceiptPlan{
		mutations: []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: value}},
		record:    receipt,
	}, nil
}
