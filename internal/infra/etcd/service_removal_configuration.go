package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// LoadServiceRemovalRecoveryConfiguration supplies immutable sources only at
// the acknowledged file-restoration boundary. The delivery owner must join the
// returned guards with authenticated-session checks before releasing plaintext.
func (repository *ServiceRepository) LoadServiceRemovalRecoveryConfiguration(ctx context.Context,
	view workqueue.ResumeView, delivery *agentpb.ServiceLifecycleWorkInput,
) (*runtimeconfiguration.Reference, []taskmaterialization.Record, []keyvalue.Condition, error) {
	if repository == nil || repository.store == nil || !view.ActiveAttempt || !view.CurrentExecutor ||
		view.Attempt.Attempt.Direction != workexecution.Recovery || view.Attempt.Attempt.StartedAt == nil ||
		view.Attempt.Attempt.FinishedAt != nil || view.Work.Task.Task.CollectionStartedAt != nil {
		return nil, nil, nil, errs.New(errs.KindStateConflict, "Removal recovery file delivery requires the active started executor")
	}
	inputs, err := serviceremovalwork.NewRepository(repository.store)
	if err != nil {
		return nil, nil, nil, err
	}
	input, inputGuard, err := inputs.Load(ctx, view.Work)
	if err != nil {
		return nil, nil, nil, err
	}
	if input.Runtime == nil || input.HostID != view.Attempt.Attempt.Executor.HostID {
		return nil, nil, nil, errs.New(errs.KindStateConflict, "Removal recovery has no captured runtime on this host")
	}
	capture, err := inputs.LoadRecovery(ctx, view.Work, delivery)
	if err != nil {
		return nil, nil, nil, err
	}
	progress, progressGuard, err := inputs.LoadRecoveryProgress(ctx, view, delivery, capture.Capture)
	if err != nil {
		return nil, nil, nil, err
	}
	if !progress.InFlight || progress.Phase != removalexecution.RestoreConfiguration {
		return nil, nil, nil, errs.New(errs.KindStateConflict, "Removal recovery is not at its acknowledged configuration step")
	}
	guards, err := inputs.RuntimeGuards(ctx, input, view.Work.ReadRevision)
	if err != nil {
		return nil, nil, nil, err
	}
	guards = append(guards, inputGuard, progressGuard,
		keyvalue.Condition{Key: serviceremovalwork.RecoveryKey(input.WorkItemID), ModRevision: capture.Revision},
		keyvalue.Condition{Key: workqueue.TaskKey(input.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(input.WorkItemID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(view.Attempt.Attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.CheckpointKey(view.Attempt.Attempt.ID), ModRevision: view.Checkpoint.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(view.Attempt.Attempt.Executor), ModRevision: view.ExecutorRevision})
	if input.Configuration == nil {
		return nil, nil, guards, nil
	}
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return nil, nil, nil, err
	}
	reference := *input.Configuration
	snapshot, err := sources.Load(ctx, reference, view.Work.ReadRevision)
	if err != nil {
		return nil, nil, nil, err
	}
	files, err := serviceruntimerecord.ConfigurationFiles(ctx, *input.Runtime, snapshot)
	if err != nil {
		return nil, nil, nil, err
	}
	return &reference, files, guards, nil
}
