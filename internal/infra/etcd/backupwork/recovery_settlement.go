package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func RecoveryReportKey(attemptID string) string {
	return "/v1/runtime/backup-work-recovery-reports/" + attemptID
}

func (repository *InputRepository) PrepareRecoverySettlement(ctx context.Context, view workqueue.ResumeView,
	raw []byte, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	assignment, guards, err := repository.LoadRecoveryAssignment(ctx, view)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	source, err := executionplan.DecodeBackupWorkRecovery(assignment.RecoverySource)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	report, evidence, err := executionplan.DecodeBackupRecoveryReport(view.Attempt.Attempt, source, raw)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if report.Operation != view.Work.Item.Item.Operation {
		return workqueue.Change{}, workexecution.Completion{}, conflict(
			"Backup recovery report operation differs from captured work",
		)
	}
	if err := report.ValidateInput(assignment.Input); err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if at.Before(report.ObservedAt) {
		return workqueue.Change{}, workexecution.Completion{}, conflict(
			"Backup recovery report observation is in the future",
		)
	}
	sources, progress, err := repository.LoadProgress(ctx, view.Work, assignment.Input)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	var change workqueue.Change
	switch view.Work.Item.Item.Operation {
	case workexecution.BackupPrune:
		change, err = repository.preparePruneReleasedResources(ctx, view.Work, assignment.Input, sources, at)
	case workexecution.Backup:
		change, err = repository.prepareCaptureRecoverySettlement(
			ctx,
			view,
			assignment.Input,
			sources,
			source,
			evidence,
			at,
		)
	case workexecution.Restore:
		change, err = repository.prepareRestoreRecoverySettlement(ctx, view.Work, assignment.Input, at)
	default:
		return workqueue.Change{}, workexecution.Completion{}, conflict(
			"Restore recovery requires its resource settlement proof",
		)
	}
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	change.Conditions = append(change.Conditions, guards.Conditions...)
	change.Conditions = append(
		change.Conditions,
		progress,
		keyvalue.Condition{Key: RecoveryReportKey(view.Attempt.Attempt.ID)},
	)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: RecoveryReportKey(view.Attempt.Attempt.ID), Value: append([]byte(nil), raw...)})
	return change, workexecution.Completion{
		Outcome:        workexecution.Succeeded,
		Evidence:       evidence,
		EffectsSettled: true,
	}, nil
}

func (repository *InputRepository) LoadRecoveryReport(ctx context.Context, view workqueue.ResumeView) ([]byte, error) {
	attempt := view.Attempt.Attempt
	if (view.Work.Item.Item.Operation != workexecution.BackupPrune && view.Work.Item.Item.Operation != workexecution.Backup && view.Work.Item.Item.Operation != workexecution.Restore) ||
		attempt.Direction != workexecution.Recovery ||
		attempt.FinishedAt == nil ||
		attempt.Result == nil ||
		attempt.Outcome != workexecution.Succeeded {
		return nil, conflict("Backup recovery has no settled report")
	}
	source, err := repository.LoadRecovery(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	read, err := repository.readSettlementKeys(ctx, []string{RecoveryReportKey(attempt.ID)}, view.Work.ReadRevision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, conflict("Backup recovery report is unavailable")
	}
	_, evidence, err := executionplan.DecodeBackupRecoveryReport(attempt, source.Source, read.Values[0].Value)
	if err != nil {
		return nil, err
	}
	if evidence != *attempt.Result {
		return nil, conflict("Backup recovery report differs from the settled evidence")
	}
	return append([]byte(nil), read.Values[0].Value...), nil
}
