package controllerupgrade

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/controller/localagent"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (coordinator *Coordinator) qualify(
	ctx context.Context,
	operation *nativeOperation,
	progress Progress,
) (workexecution.Outcome, error) {
	expected := operation.expected
	if coordinator.process != expected.Manifest.ControllerSHA256 {
		return coordinator.requestRollback(
			ctx,
			operation,
			errs.New(errs.KindStateConflict, "native candidate process differs"),
			progress,
		)
	}
	qualification, cancel := context.WithTimeout(ctx, expected.CandidateDeadline().Sub(coordinator.now()))
	defer cancel()
	err := progress.Run(qualification, "controller_candidate_check", coordinator.readiness.Ready)
	if err == nil {
		err = coordinator.recoverAgent(qualification, operation, localagent.UpdateFinish, progress)
	}
	if err == nil {
		err = progress.Run(qualification, "controller_final_check", coordinator.readiness.Ready)
	}
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return coordinator.requestRollback(ctx, operation, err, progress)
	}
	if err := qualification.Err(); err != nil {
		return coordinator.requestRollback(ctx, operation, err, progress)
	}
	if _, err := coordinator.journal.Advance(
		qualification, expected.TaskID, upgrade.PhaseStarting, upgrade.PhaseHealthy,
	); err != nil {
		return "", err
	}
	return coordinator.settle(operation, workexecution.Succeeded), nil
}

func (coordinator *Coordinator) confirmHealthy(
	ctx context.Context, operation *nativeOperation,
	progress Progress,
) (workexecution.Outcome, error) {
	if coordinator.process != operation.expected.Manifest.ControllerSHA256 {
		return "", errs.New(errs.KindStateConflict, "qualified controller process identity differs")
	}
	ctx, cancel := context.WithTimeout(ctx, upgrade.RecoveryReserveSeconds*time.Second)
	defer cancel()
	if err := progress.Run(ctx, "controller_candidate_check", coordinator.readiness.Ready); err != nil {
		return "", err
	}
	if err := coordinator.recoverAgent(ctx, operation, localagent.UpdateFinish, progress); err != nil {
		return "", err
	}
	return coordinator.settle(operation, workexecution.Succeeded), nil
}

func (coordinator *Coordinator) recoverPredecessor(
	ctx context.Context, operation *nativeOperation, phase upgrade.Phase,
	progress Progress,
) (workexecution.Outcome, error) {
	if coordinator.process != operation.expected.PreviousController {
		return "", errs.New(errs.KindStateConflict, "recovered controller process identity differs")
	}
	ctx, cancel := context.WithTimeout(ctx, upgrade.RecoveryReserveSeconds*time.Second)
	defer cancel()
	if err := progress.Run(ctx, "controller_restore_check", coordinator.readiness.Ready); err != nil {
		return "", err
	}
	if err := coordinator.recoverAgent(ctx, operation, localagent.UpdateRestore, progress); err != nil {
		return "", err
	}
	if phase == upgrade.PhaseRolledBack {
		if _, err := coordinator.journal.Advance(
			ctx, operation.expected.TaskID, upgrade.PhaseRolledBack, upgrade.PhaseRecovered,
		); err != nil {
			return "", err
		}
	}
	return coordinator.settle(operation, workexecution.Failed), nil
}

func (coordinator *Coordinator) recoverAgent(
	ctx context.Context, operation *nativeOperation, goal localagent.UpdateGoal,
	progress Progress,
) error {
	stage := "agent_update"
	if goal == localagent.UpdateRestore {
		stage = "agent_restore"
	} else if operation.expected.Agent != nil && operation.expected.Agent.Image == operation.expected.Manifest.AgentImage {
		stage = "agent_ready_check"
	}
	if operation.expected.Agent == nil {
		return coordinator.reconcileAgent(ctx, operation, goal)
	}
	return progress.Run(ctx, stage, func(ctx context.Context) error {
		return coordinator.reconcileAgent(ctx, operation, goal)
	})
}

func (coordinator *Coordinator) reconcileAgent(
	ctx context.Context, operation *nativeOperation, goal localagent.UpdateGoal,
) error {
	expected := operation.expected
	if expected.Agent == nil {
		agents, err := coordinator.agents.ListHealth(ctx)
		if err != nil {
			return err
		}
		if len(agents) != 0 {
			return errs.New(errs.KindStateConflict, "controller update Agent selection changed")
		}
		return nil
	}
	agent := expected.Agent
	desired := expected.Manifest.AgentImage
	if agent.Image != desired {
		result, err := coordinator.agents.RecoverUpdate(ctx, localagent.UpdateRequest{
			AgentID: agent.ID, PreviousImage: agent.Image, DesiredImage: desired, StartingGeneration: agent.Generation,
		}, goal, nil)
		if err != nil {
			return err
		}
		wanted := localagent.UpdateReadyDesired
		if goal == localagent.UpdateRestore {
			wanted = localagent.UpdateReadyPrevious
		}
		if result != wanted {
			return errs.New(errs.KindStateConflict, "controller update Agent recovered another image")
		}
	}
	if goal == localagent.UpdateRestore {
		desired = agent.Image
	}
	for {
		health, err := coordinator.agents.Health(ctx, agent.ID)
		if err != nil {
			return err
		}
		generation := agent.Generation
		if agent.Image != expected.Manifest.AgentImage {
			if goal == localagent.UpdateFinish {
				generation++
			} else if health.Agent.Generation != agent.Generation {
				generation += 2
			}
		}
		if health.Agent.ID != agent.ID || health.Agent.Generation != generation || health.Agent.Image != desired ||
			health.Agent.Phase != localagent.PhaseReady {
			return errs.New(errs.KindStateConflict, "controller update Agent readiness identity changed")
		}
		if health.Healthy {
			return ctx.Err()
		}
		if err := waitPass(ctx); err != nil {
			return err
		}
	}
}

func (coordinator *Coordinator) requestRollback(
	ctx context.Context, operation *nativeOperation, cause error,
	progress Progress,
) (workexecution.Outcome, error) {
	coordinator.logger.Error("native controller candidate requires recovery", "task_id", operation.expected.TaskID,
		"error", cause)
	journal, found, err := coordinator.current(ctx, operation)
	if err != nil {
		return "", err
	}
	if !found || journal.Phase.Settled() {
		return "", errs.New(errs.KindStateConflict, "native rollback lost journal authority")
	}
	if journal.Phase != upgrade.PhaseRollingBack && journal.Phase != upgrade.PhaseStopping &&
		journal.Phase != upgrade.PhaseRolledBack {
		if _, err := coordinator.journal.BeginRollback(ctx, journal.TaskID, journal.Phase,
			taskprogress.CaptureControllerFailure(cause)); err != nil {
			return "", err
		}
	}
	return coordinator.handoff(ctx, operation, progress)
}
