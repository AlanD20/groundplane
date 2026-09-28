package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A terminal Task is not a reference to every image on the host. Resolve its
// immutable inputs, including predecessor runtime, before releasing that broad
// barrier. Unknown procedures keep the barrier; missing/corrupt inputs fail closed.
func (service *Service) retainTaskImages(ctx context.Context, retention *imageRetention, task etcd.TaskRecord, revision int64) (bool, error) {
	if !taskjournal.IsTerminalTaskStatus(task.Status) {
		return false, nil
	}
	if task.Executor == taskjournal.TaskExecutorController && task.Type == taskjournal.TaskCreate &&
		task.Params[taskjournal.TaskResourceKindParam] == runners.TaskResourceRunner {
		// Fresh-token retry uses the retained Runner, scanned independently.
		return true, nil
	}
	if task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskUpdate {
		return false, nil
	}
	if task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceComponent {
		return true, service.retainComponentTaskImages(ctx, retention, task, revision)
	}
	if ids.Validate(ids.KindEnvironment, task.Target) == nil {
		if _, valid := taskcontract.ParseBlueprintComposeProcedure(task.Params[taskcontract.EnvironmentBlueprintProcedureParam]); valid {
			return true, service.retainBlueprintTaskImages(ctx, retention, task, revision)
		}
	}
	return false, nil
}

func (service *Service) retainComponentTaskImages(ctx context.Context, retention *imageRetention, task etcd.TaskRecord, revision int64) error {
	key := platformcomponents.PlatformComponentTaskRenderInputKey(task.PlanID)
	read, err := service.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil || read.Values[0].Key != key {
		return errs.New(errs.KindInternal, "retained Component Task image input is missing")
	}
	input, err := platformcomponents.DecodePlatformComponentTaskRenderInput(read.Values[0].Value)
	if err != nil {
		return err
	}
	if input.PlanID != task.PlanID || input.ComponentID != task.Target || input.ExecutionPlanSHA256 != task.PlanHash ||
		(input.TaskID != task.ID && task.RetryOf == "") {
		return errs.New(errs.KindInternal, "retained Component Task image input differs from its Task")
	}
	reason := "Retained Task " + task.ID
	retention.retain(input.ImageReference, reason)
	retention.retain("sha256:"+input.ImageConfigDigest, reason)
	retention.composeArtifact(input.ComposeArtifact, reason)
	retention.composeArtifact(input.RollbackComposeArtifact, reason)
	return nil
}

func (service *Service) retainBlueprintTaskImages(ctx context.Context, retention *imageRetention, task etcd.TaskRecord, revision int64) error {
	reason := "Retained Task " + task.ID
	retainProjection := func(environmentID, revisionID string) error {
		projection, found, err := blueprints.ReadEffectiveProjectionRevision(ctx, service.store, environmentID, revisionID, revision)
		if err != nil {
			return err
		}
		if !found || projection.ReadRevision != revision {
			return errs.New(errs.KindInternal, "retained Blueprint Task image input is missing")
		}
		if revisionID == task.Params[blueprints.EnvironmentDesiredRevisionParam] && projection.Record.RenderGeneration != uint64(task.RenderGeneration) {
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
