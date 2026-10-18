package softwarepreparation

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type preparationObserver struct {
	repository *executiondiagnostics.Repository
	attempt    workexecution.Attempt
	sequence   uint64
	lost       bool
}

func (observer *preparationObserver) Run(ctx context.Context, key string, action func(context.Context) error) error {
	name, expected, err := preparationStage(key)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	stage := workexecution.Stage{Key: key, Name: name, Expected: expected, State: workexecution.StageRunning, StartedAt: &at}
	observer.record(ctx, stage)
	executionErr := action(ctx)
	finished := time.Now().UTC()
	stage.FinishedAt = &finished
	stage.State, stage.Observed = workexecution.StageSucceeded, "Completed the captured operation."
	if executionErr != nil {
		stage.State = workexecution.StageFailed
		stage.FailureCode, stage.Observed = publicDiagnostic(executionErr)
	}
	observer.record(ctx, stage)
	return executionErr
}

func (observer *preparationObserver) record(ctx context.Context, stage workexecution.Stage) {
	if observer.lost {
		return
	}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	attempt := observer.attempt
	if err := observer.repository.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID,
		attempt.Executor, observer.sequence+1, stage, time.Now().UTC()); err != nil {
		observer.lost = true
		slog.WarnContext(ctx, "software execution diagnostics incomplete", "task_id", attempt.TaskID, "attempt_id", attempt.ID, "error", err)
		loss, finish := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer finish()
		if err := observer.repository.MarkIncomplete(loss, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); err != nil {
			slog.WarnContext(ctx, "unable to persist software diagnostic loss", "attempt_id", attempt.ID, "error", err)
		}
		return
	}
	observer.sequence++
}

func preparationStage(key string) (string, string, error) {
	switch key {
	case "software_checkpoint_check":
		return "Verify previously published artifacts", "Read back exact captured registry manifests and configuration digests", nil
	case "software_source_download":
		return "Download captured source revision", "Download and validate only the resolved commit archive", nil
	case "software_controller_build":
		return "Build Controller bundle", "Build the captured source and tool catalog inside the isolated builder", nil
	case "software_agent_build":
		return "Build Agent image", "Build the captured source revision for the selected host architecture", nil
	case "software_release_import":
		return "Import published release", "Verify the captured component release checksums and immutable artifact identities", nil
	case "software_agent_publish":
		return "Publish Agent image", "Push the Agent artifact to GP's registry and verify its readback", nil
	case "software_controller_publish":
		return "Publish Controller bundle", "Push the Controller artifact to GP's registry and verify its readback", nil
	case "software_output_check":
		return "Verify prepared artifacts", "Verify all selected immutable outputs after retiring builder resources", nil
	default:
		return "", "", errs.New(errs.KindInternal, "software preparation stage is not registered")
	}
}
