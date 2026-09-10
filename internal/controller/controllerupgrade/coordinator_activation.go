package controllerupgrade

import (
	"context"
	"errors"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (coordinator *Coordinator) prepare(
	ctx context.Context, operation *nativeOperation, prepared bool,
) (etcd.TaskStatus, error) {
	expected := operation.expected
	budget := min(expected.CandidateDeadline().Sub(coordinator.now()), upgrade.DrainTimeoutSeconds*time.Second)
	preparation, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	coordinator.mu.Lock()
	operation.cancel = cancel
	if operation.aborted {
		cancel()
	}
	coordinator.mu.Unlock()
	defer func() {
		coordinator.mu.Lock()
		operation.cancel = nil
		coordinator.mu.Unlock()
	}()
	preparationErr := coordinator.validateAndDrain(preparation, operation)
	if preparationErr == nil {
		preparationErr = coordinator.journal.Prepare(preparation, expected)
	}
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if preparationErr != nil {
		coordinator.logger.Error("native controller update preparation failed", "task_id", expected.TaskID,
			"error", preparationErr)
		journal, found, err := coordinator.current(ctx, operation)
		if err != nil {
			return "", err
		}
		if found {
			if journal.Phase == upgrade.PhaseCancelled {
				return coordinator.settle(operation, etcd.TaskStatusAborted), nil
			}
			// A copy/fsync response may be lost after journal publication.
			// Retain its claim rather than treating the failure as no effect.
			return "", preparationErr
		}
		if prepared {
			return "", errs.New(errs.KindStateConflict, "native preparation journal disappeared")
		}
		coordinator.mu.Lock()
		aborted := operation.aborted
		coordinator.mu.Unlock()
		status := etcd.TaskStatusFailed
		if aborted {
			status = etcd.TaskStatusAborted
		} else if !coordinator.now().Before(expected.Deadline) {
			status = etcd.TaskStatusTimedOut
		}
		return coordinator.settle(operation, status), nil
	}
	coordinator.mu.Lock()
	phase := upgrade.PhaseActivating
	if operation.aborted {
		phase = upgrade.PhaseCancelled
	} else if !coordinator.now().Before(expected.CandidateDeadline()) {
		phase = upgrade.PhaseRollingBack
	}
	_, err := coordinator.journal.Advance(ctx, expected.TaskID, upgrade.PhasePrepared, phase)
	coordinator.mu.Unlock()
	if err != nil {
		return "", err
	}
	if phase == upgrade.PhaseCancelled {
		return coordinator.settle(operation, etcd.TaskStatusAborted), nil
	}
	return coordinator.handoff(ctx, operation)
}

func (coordinator *Coordinator) validateAndDrain(ctx context.Context, operation *nativeOperation) error {
	expected := operation.expected
	if err := ctx.Err(); err != nil {
		return err
	}
	if coordinator.process != expected.PreviousController {
		return errs.New(errs.KindStateConflict, "native predecessor process identity changed")
	}
	installed, err := coordinator.journal.Installed(ctx)
	if err != nil {
		return err
	}
	if installed != expected.PreviousController {
		return errs.New(errs.KindStateConflict, "native installed predecessor identity changed")
	}
	manifest, err := coordinator.journal.Inspect(ctx, expected.Release)
	if err != nil {
		return err
	}
	if manifest != expected.Manifest {
		return errs.New(errs.KindStateConflict, "native candidate differs from the Task")
	}
	if err := coordinator.unit.VerifyBootstrap(ctx); err != nil {
		return err
	}
	if err := coordinator.readiness.Ready(ctx); err != nil {
		return err
	}
	agents, err := coordinator.agents.ListHealth(ctx)
	if err != nil {
		return err
	}
	if expected.Agent == nil {
		if len(agents) != 0 {
			return errs.New(errs.KindStateConflict, "native update Agent selection changed")
		}
		return nil
	}
	if len(agents) != 1 || agents[0].Agent.ID != expected.Agent.ID ||
		agents[0].Agent.Generation != expected.Agent.Generation || agents[0].Agent.Image != expected.Agent.Image ||
		agents[0].Agent.Phase != localagent.PhaseReady {
		return errs.New(errs.KindStateConflict, "native update predecessor Agent changed")
	}
	maximum := agents[0].Agent.Config.MaxConcurrentTasks
	deadline := minTime(expected.CandidateDeadline(), coordinator.now().Add(upgrade.DrainTimeoutSeconds*time.Second))
	for {
		err := coordinator.work.RequireIdle(ctx, expected.Agent.ID, expected.Agent.Generation, maximum)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) || !coordinator.now().Before(deadline) {
			return err
		}
		if err := waitPass(ctx); err != nil {
			return err
		}
	}
}

func (coordinator *Coordinator) Abort(ctx context.Context, taskID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	operation := coordinator.active
	if operation == nil || operation.expected.TaskID != taskID || operation.settled != "" {
		return errs.New(errs.KindStateConflict, "native controller update Task is not active")
	}
	journal, found, err := coordinator.current(ctx, operation)
	if err != nil {
		return err
	}
	if found {
		switch journal.Phase {
		case upgrade.PhasePrepared:
			if _, err := coordinator.journal.Advance(ctx, taskID, upgrade.PhasePrepared, upgrade.PhaseCancelled); err != nil {
				return err
			}
		case upgrade.PhaseCancelled:
		default:
			return errs.New(
				errs.KindResourceInUse,
				"native controller activation is committed; recovery cannot be aborted",
			)
		}
	}
	operation.aborted = true
	if operation.cancel != nil {
		operation.cancel()
	}
	return nil
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}
