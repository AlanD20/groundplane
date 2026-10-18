package controllerupgrade

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type workObserver struct {
	repository   *executiondiagnostics.Repository
	attempt      workexecution.Attempt
	ctx          context.Context
	terminalOnly bool
	sequence     uint64
	lost         bool
	stages       []workexecution.Stage
}

func (observer *workObserver) Start(key string) error {
	_, err := observer.start(observer.ctx, key)
	return err
}

func (observer *workObserver) start(ctx context.Context, key string) (workexecution.Stage, error) {
	name, expected, err := upgradeStage(key)
	if err != nil {
		return workexecution.Stage{}, err
	}
	at := time.Now().UTC()
	stage := workexecution.Stage{Key: key, Name: name, Expected: expected, State: workexecution.StageRunning, StartedAt: &at}
	observer.record(ctx, stage)
	return stage, nil
}

func (observer *workObserver) Run(ctx context.Context, key string, action func(context.Context) error) error {
	stage, err := observer.start(ctx, key)
	if err != nil {
		return err
	}
	executionErr := action(ctx)
	finished := time.Now().UTC()
	stage.FinishedAt = &finished
	stage.State, stage.Observed = workexecution.StageSucceeded, "The captured operation completed."
	if executionErr != nil {
		failure := taskprogress.CaptureControllerFailure(executionErr)
		stage.State, stage.FailureCode, stage.Observed = workexecution.StageFailed, failure.Code, failure.Summary()
	}
	observer.record(ctx, stage)
	return executionErr
}

func (observer *workObserver) record(ctx context.Context, stage workexecution.Stage) {
	if observer.lost {
		return
	}
	if observer.terminalOnly {
		if len(observer.stages) >= 32 {
			observer.lost = true
			return
		}
		observer.stages = append(observer.stages, stage)
		return
	}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	attempt := observer.attempt
	err := observer.repository.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor, observer.sequence+1, stage, time.Now().UTC())
	if err == nil {
		observer.sequence++
		return
	}
	observer.lost = true
	slog.WarnContext(ctx, "Controller activation diagnostics incomplete", "task_id", attempt.TaskID, "attempt_id", attempt.ID, "error", err)
	loss, finish := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer finish()
	if err := observer.repository.MarkIncomplete(loss, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); err != nil {
		slog.WarnContext(ctx, "unable to record Controller activation diagnostic loss", "attempt_id", attempt.ID, "error", err)
	}
}

func upgradeStage(key string) (string, string, error) {
	switch key {
	case "controller_update_prepare":
		return "Validate release and drain Agent work", "Verify the captured predecessor, candidate manifest and bootstrap; wait for the selected Agent's work to settle", nil
	case "controller_update_stage":
		return "Stage native handoff journal", "Durably capture the selected release and exact Controller/Agent predecessors before cutover", nil
	case "controller_handoff":
		return "Start guarded Controller replacement", "Launch the independent native watchdog for this captured operation", nil
	case "controller_wait_restart":
		return "Wait for replacement process", "The outgoing Controller must stop; the replacement process owns qualification", nil
	case "controller_candidate_check":
		return "Check replacement Controller", "Verify the running candidate and healthy API, Agent channel and etcd connection", nil
	case "controller_final_check":
		return "Qualify Controller activation", "Recheck Controller readiness after restoring the captured Agent state", nil
	case "controller_restore_check":
		return "Check restored Controller", "Verify the captured previous Controller process and its listeners/storage", nil
	case "agent_update":
		return "Activate captured Agent image", "Reconcile only the image and generation captured by this Controller activation", nil
	case "agent_restore":
		return "Restore captured Agent predecessor", "Restore the original Agent image and verify its authenticated readiness", nil
	case "agent_ready_check":
		return "Verify preserved Agent", "Verify the captured Agent identity, image, generation and authenticated readiness", nil
	default:
		return "", "", errs.New(errs.KindInternal, "Controller activation stage is not registered")
	}
}

// Reconciliation does not impersonate the old executor to emit live reports.
// Keep a bounded local observation batch, publishing it only against the exact
// accepted terminal result. Losing this batch affects diagnostics, not authority.
func (owner *Work) rememberObservations(attemptID string, observer *workObserver) {
	if !observer.terminalOnly || len(observer.stages) == 0 {
		return
	}
	owner.observationsMu.Lock()
	defer owner.observationsMu.Unlock()
	if len(owner.observations) >= 256 {
		return
	}
	owner.observations[attemptID] = observer.stages
}

func (owner *Work) publishObservations(ctx context.Context, attempt workexecution.Attempt) {
	owner.observationsMu.Lock()
	stages := owner.observations[attempt.ID]
	delete(owner.observations, attempt.ID)
	owner.observationsMu.Unlock()
	if len(stages) == 0 {
		return
	}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	snapshot, err := owner.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
	if err != nil {
		slog.WarnContext(ctx, "unable to read Controller reconciliation diagnostics", "attempt_id", attempt.ID, "error", err)
		return
	}
	sequence := uint64(0)
	if snapshot.Record != nil {
		sequence = snapshot.Record.Sequence
	}
	for _, stage := range stages {
		if err := owner.diagnostics.ReportResult(bounded, attempt, sequence+1, stage); err != nil {
			slog.WarnContext(ctx, "unable to publish Controller reconciliation diagnostics", "attempt_id", attempt.ID, "error", err)
			return
		}
		sequence++
	}
}
