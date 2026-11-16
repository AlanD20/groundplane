package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// PublishWork consumes the exact resource snapshot once. It joins native work
// admission with the prepared resource transaction; no legacy Task is emitted.
func (publication *PreparedBackupRunPublication) PrepareWorkAuthority(ctx context.Context,
	task workexecution.Task, item workexecution.Item, input *agentpb.BackupWorkInput,
) (workqueue.Change, error) {
	_, authority, err := publication.prepareWorkAuthority(ctx, task, item, input)
	return authority, err
}

// PublishWork is the internal scheduled-Backup publisher. Manual actions use
// PrepareWorkAuthority and promote their existing accepted Task.
func (publication *PreparedBackupRunPublication) PublishWork(
	ctx context.Context,
	task workexecution.Task,
	item workexecution.Item,
	input *agentpb.BackupWorkInput,
	marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if task.Actor != workexecution.SystemActor {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Scheduled Backup requires a system Task",
		)
	}
	repository, authority, err := publication.prepareWorkAuthority(ctx, task, item, input)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(authority.Mutations)
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(work.Mutations)
	joined, err := workqueue.JoinChanges(authority, work)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newIdempotencyMutationPlanForMarker(
		idempotency.IdempotencyMarkerTask,
		joined.Conditions,
		joined.Mutations,
		func(_ int64, _ []*keyvalue.KeyValue) error {
			return errs.New(errs.KindStateConflict, "Scheduled Backup publication changed")
		},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return (&IdempotencyRepository{store: repository.store}).Apply(ctx, marker, plan)
}

func (publication *PreparedBackupRunPublication) prepareWorkAuthority(ctx context.Context,
	task workexecution.Task, item workexecution.Item, input *agentpb.BackupWorkInput,
) (*BackupRuntimeRepository, workqueue.Change, error) {
	if publication == nil || publication.state == nil {
		return nil, workqueue.Change{}, errs.New(errs.KindInternal, "Backup publication is not prepared")
	}
	state := publication.state
	state.mu.Lock()
	if state.consumed || state.repository == nil {
		state.mu.Unlock()
		return nil, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Backup publication was already consumed",
		)
	}
	state.consumed = true
	repository, plan := state.repository, state.plan
	scope, steps, artifacts := state.scope, state.authority, state.artifacts
	state.repository = nil
	state.plan = backupRunPublicationPlan{}
	state.scope, state.authority, state.artifacts = nil, nil, nil
	state.mu.Unlock()
	defer plan.clear()
	if input == nil || scope == nil || input.ProjectId != scope.ProjectId ||
		input.EnvironmentId != scope.EnvironmentId ||
		!proto.Equal(input.Project, scope.Project) ||
		!proto.Equal(input.Environment, scope.Environment) ||
		len(input.Services) != len(scope.Services) ||
		len(input.Steps) != len(steps) ||
		len(input.Artifacts) != len(artifacts) {
		return nil, workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Backup captured resource scope changed",
		)
	}
	for index, fact := range scope.Services {
		if !proto.Equal(fact, input.Services[index]) {
			return nil, workqueue.Change{}, errs.New(
				errs.KindValidationFailed,
				"Backup captured Service fact changed",
			)
		}
	}
	for index, step := range steps {
		if !proto.Equal(step, input.Steps[index]) {
			return nil, workqueue.Change{}, errs.New(
				errs.KindValidationFailed,
				"Backup captured source authority changed",
			)
		}
	}
	for index, artifact := range artifacts {
		if !proto.Equal(artifact, input.Artifacts[index]) {
			return nil, workqueue.Change{}, errs.New(
				errs.KindValidationFailed,
				"Backup captured artifact changed",
			)
		}
	}
	if item.Placement.HostID != plan.hostID || plan.hostID == "" {
		return nil, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Backup target host changed after capture",
		)
	}

	if task.Scope.TenantID != plan.initiation.owner.TenantID ||
		task.Scope.ProjectID != plan.initiation.owner.ProjectID ||
		task.Scope.EnvironmentID != plan.initiation.owner.EnvironmentID {
		return nil, workqueue.Change{}, errs.New(errs.KindStateConflict, "Backup ancestry changed")
	}
	native, err := backupwork.PrepareCaptureAuthority(task, item, input, plan.record)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	defer keyvalue.ClearMutationValues(native.Mutations)
	conditions, mutations, err := plan.composeTransaction(native.Conditions, native.Mutations)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	defer keyvalue.ClearMutationValues(mutations)
	conditions, _, err = prepareTaskInitiationFences(plan.initiation, conditions,
		func(_ int64, _ []*keyvalue.KeyValue) error {
			return errs.New(errs.KindStateConflict, "Backup sources changed during preparation")
		})
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	return repository, workqueue.Change{Conditions: conditions, Mutations: keyvalue.CloneMutations(mutations)}, nil
}
