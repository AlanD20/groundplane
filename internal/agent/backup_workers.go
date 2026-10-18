package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/agent/backupruntime"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (workers *executionWorkers) submitBackup(delivery *agentpb.BackupWorkAssignment, available int, staging *backupruntime.Staging) error {
	if staging == nil {
		return errs.New(errs.KindStateConflict, "Backup startup recovery has not completed")
	}
	owned, attempt, err := executionplan.OpenBackupWorkAssignment(delivery, workers.identity)
	if err != nil {
		return err
	}
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Backup worker session is closed")
	}
	if active, exists := workers.active[attempt.WorkItemID]; exists {
		if active.backup == nil || !proto.Equal(active.backup, owned) {
			return errs.New(errs.KindStateConflict, "Backup work already has a different active assignment")
		}
		return nil
	}
	if available <= 0 || len(workers.active) >= workers.limit {
		return errs.New(errs.KindStateConflict, "Agent execution capacity is exhausted")
	}
	execution, err := workers.ownership.Begin(attempt)
	if err != nil {
		return err
	}
	if err := workers.exchange.RegisterBackupVolumes(owned); err != nil {
		return errors.Join(err, execution.Close())
	}
	if err := workers.exchange.RegisterBackupConfigs(owned); err != nil {
		workers.exchange.ReleaseBackupVolumes(attempt.ID)
		return errors.Join(err, execution.Close())
	}
	workers.active[attempt.WorkItemID] = activeExecution{attempt: attempt, backup: owned}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		defer workers.exchange.ReleaseBackupVolumes(attempt.ID)
		defer workers.exchange.ReleaseBackupConfigs(attempt.ID)
		terminal, err := workers.backups.Run(workers.ctx, owned, workers.identity, workers.exchange, staging)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: attempt, backup: &terminal, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}

func (workers *executionWorkers) consumeBackup(output executionOutput) error {
	if output.err == nil {
		if output.backup == nil || output.backup.Validate() != nil || output.backup.FinishedAt == nil ||
			output.backup.Result == nil || !workexecution.SameAssignment(*output.backup, output.attempt) {
			return errs.New(errs.KindStateConflict, "Backup result requires its saved terminal acknowledgement")
		}
		if err := workers.backups.Retire(workers.ctx, *output.backup); err != nil {
			return err
		}
	}
	delete(workers.active, output.attempt.WorkItemID)
	return nil
}

func (workers *executionWorkers) submitBackupRecovery(delivery *agentpb.BackupRecoveryAssignment, available int, staging *backupruntime.Staging) error {
	owned, attempt, _, err := executionplan.OpenBackupRecoveryAssignment(delivery, workers.identity)
	if err != nil {
		return err
	}
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Backup recovery worker session is closed")
	}
	if active, exists := workers.active[attempt.WorkItemID]; exists {
		if active.backupRecovery == nil || !proto.Equal(active.backupRecovery, owned) {
			return errs.New(errs.KindStateConflict, "Backup work already has a different active recovery")
		}
		return nil
	}
	if available <= 0 || len(workers.active) >= workers.limit {
		return errs.New(errs.KindStateConflict, "Agent recovery capacity is exhausted")
	}
	execution, err := workers.ownership.Begin(attempt)
	if err != nil {
		return err
	}
	workers.active[attempt.WorkItemID] = activeExecution{attempt: attempt, backupRecovery: owned}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		terminal, err := workers.backups.RunRecovery(workers.ctx, owned, workers.identity, workers.exchange, workers.ownership, staging)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: attempt, backup: &terminal, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}
