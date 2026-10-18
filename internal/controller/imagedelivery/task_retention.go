package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A terminal Task is not a reference to every image on the host. Resolve its
// immutable inputs, including predecessor runtime, before releasing that broad
// barrier. Unknown procedures keep the barrier; missing/corrupt inputs fail closed.
func (service *Service) retainTaskImages(
	ctx context.Context,
	retention *imageRetention,
	task etcd.TaskRecord,
	revision int64,
) (bool, error) {
	if !taskjournal.IsTerminalTaskStatus(task.Status) {
		return false, nil
	}
	if task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceSoftware {
		// Durable preparation/activation records retain exact outputs and
		// predecessors independently, including terminal partial failures.
		return true, nil
	}
	if task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskUpdate {
		return false, nil
	}
	if ids.Validate(ids.KindEnvironment, task.Target) == nil {
		if _, valid := taskcontract.ParseBlueprintComposeProcedure(task.Params[taskcontract.EnvironmentBlueprintProcedureParam]); valid {
			return true, service.retainBlueprintTaskImages(ctx, retention, task, revision)
		}
	}
	return false, nil
}

func (service *Service) retainBlueprintTaskImages(
	ctx context.Context,
	retention *imageRetention,
	task etcd.TaskRecord,
	revision int64,
) error {
	reason := "Retained Task " + task.ID
	retainProjection := func(environmentID, revisionID string) error {
		projection, found, err := blueprints.ReadEffectiveProjectionRevision(
			ctx,
			service.store,
			environmentID,
			revisionID,
			revision,
		)
		if err != nil {
			return err
		}
		if !found || projection.ReadRevision != revision {
			return errs.New(errs.KindInternal, "retained Blueprint Task image input is missing")
		}
		if revisionID == task.Params[blueprints.EnvironmentDesiredRevisionParam] &&
			projection.Record.RenderGeneration != uint64(task.RenderGeneration) {
			return errs.New(errs.KindInternal, "retained Blueprint Task image generation differs from its Task")
		}
		return retention.artifact(projection.Record.ComposeArtifact, reason)
	}
	if err := retainProjection(task.Target, task.Params[blueprints.EnvironmentDesiredRevisionParam]); err != nil {
		return err
	}
	// A removed/replaced Component can still be restored from an older revision.
	for _, source := range task.ManagedComponentTeardownSources {
		if err := retainProjection(task.Target, source.RevisionID); err != nil {
			return err
		}
	}
	// Candidate workload, prior runtime and hook images are retained separately
	// through all sealed Release inputs, including failed Blueprint candidates.
	return nil
}
