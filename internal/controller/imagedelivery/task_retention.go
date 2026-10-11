package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
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
func (service *Service) retainTaskImages(
	ctx context.Context,
	retention *imageRetention,
	task etcd.TaskRecord,
	revision int64,
) (bool, error) {
	if task.Type == taskjournal.TaskCheck && task.Executor == taskjournal.TaskExecutorController {
		switch task.Params[taskjournal.TaskResourceKindParam] {
		case taskjournal.TaskResourceConnector:
			return true, nil // Controller storage checks use no host image.
		case taskjournal.TaskResourceAttach:
			return true, service.retainBackingCheckImages(ctx, retention, task, revision)
		case taskjournal.TaskResourceRoute:
			return true, service.retainEdgeCheckImages(ctx, retention, task, revision)
		}
	}
	if !taskjournal.IsTerminalTaskStatus(task.Status) {
		return false, nil
	}
	if resource := task.Params[taskjournal.TaskResourceKindParam]; resource == taskjournal.TaskResourceSoftwarePreparation ||
		resource == taskjournal.TaskResourceSoftware {
		// Durable preparation/activation records retain exact outputs and
		// predecessors independently, including terminal partial failures.
		return true, nil
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

func (service *Service) retainBackingCheckImages(
	ctx context.Context,
	retention *imageRetention,
	task etcd.TaskRecord,
	revision int64,
) error {
	keys := []string{backingchecks.Key(task.ID), backingchecks.OperationKey(task.OperationID)}
	read, err := service.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return errs.New(errs.KindInternal, "Backing check image authority read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		if taskjournal.IsTerminalTaskStatus(task.Status) && read.Values[1] == nil {
			return nil
		}
		return errs.New(errs.KindStateConflict, "Backing check still requires its captured helper image authority")
	}
	record, err := backingchecks.Decode(read.Values[0].Value)
	if err != nil {
		return err
	}
	defer record.Clear()
	hash, err := backingchecks.PlanHash(record)
	if err != nil || record.Owner.TaskID != task.ID || record.Owner.PlanID != task.PlanID ||
		record.Owner.OperationID != task.OperationID ||
		record.Owner.EnvironmentID != task.Owner.EnvironmentID ||
		record.Source.Parameters.AttachID != task.Target ||
		record.ConfigurationSHA256 != task.Params[taskjournal.TaskBackingCheckInputParam] ||
		hash != task.PlanHash {
		return errs.New(errs.KindStateConflict, "Backing check image authority differs from its Task")
	}
	if record.Phase != backingchecks.Finished && record.Phase != backingchecks.Transferred {
		reason := "Access check helper retained by Task " + task.ID
		retention.retain(record.Source.HelperImageID, reason)
		retention.retain(record.Source.HelperImage, reason)
	}
	return nil
}

func (service *Service) retainComponentTaskImages(
	ctx context.Context,
	retention *imageRetention,
	task etcd.TaskRecord,
	revision int64,
) error {
	key := platformcomponents.PlatformComponentTaskRenderInputKey(task.PlanID)
	read, err := service.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key {
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
