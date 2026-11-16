package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type restoreWorkPublication struct {
	hostID     string
	owner      taskjournal.TaskOwner
	restore    backupruntime.BackupRestoreRecord
	scope      *agentpb.BackupPlanScope
	authority  *agentpb.BackupStepAuthority
	artifacts  []*agentpb.ComposeArtifact
	conditions []keyvalue.Condition
	mutations  []keyvalue.Mutation
}

func (repository *BackupRuntimeRepository) prepareRestoreWorkAuthority(ctx context.Context,
	captured restoreWorkPublication, task workexecution.Task, item workexecution.Item,
	input *agentpb.BackupWorkInput,
) (workqueue.Change, error) {
	defer keyvalue.ClearMutationValues(captured.mutations)
	invalid := func() (workqueue.Change, error) {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Restore captured publication changed",
		)
	}
	if input == nil || captured.scope == nil || captured.hostID == "" || item.Placement.HostID != captured.hostID ||
		task.Scope.TenantID != captured.owner.TenantID || task.Scope.ProjectID != captured.owner.ProjectID ||
		task.Scope.EnvironmentID != captured.owner.EnvironmentID || input.ProjectId != captured.scope.ProjectId ||
		input.EnvironmentId != captured.scope.EnvironmentId || !proto.Equal(input.Project, captured.scope.Project) ||
		!proto.Equal(
			input.Environment,
			captured.scope.Environment,
		) || len(input.Services) != len(captured.scope.Services) ||
		len(
			input.Steps,
		) != 1 || !proto.Equal(input.Steps[0], captured.authority) || len(input.Artifacts) != len(captured.artifacts) {
		return invalid()
	}
	for index, fact := range captured.scope.Services {
		if !proto.Equal(fact, input.Services[index]) {
			return invalid()
		}
	}
	for index, artifact := range captured.artifacts {
		if !proto.Equal(artifact, input.Artifacts[index]) {
			return invalid()
		}
	}
	change, err := backupwork.PrepareRestoreAuthority(task, item, input, captured.restore)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	conditions := append(change.Conditions, captured.conditions...)
	mutations := append(keyvalue.CloneMutations(change.Mutations), keyvalue.CloneMutations(captured.mutations)...)
	defer keyvalue.ClearMutationValues(mutations)
	initiation, err := newTaskInitiation(captured.owner, taskjournal.TaskActorOperator)
	if err != nil {
		return workqueue.Change{}, err
	}
	classify := idempotencyPlanClassifier(func(_ int64, _ []*keyvalue.KeyValue) error {
		return errs.New(
			errs.KindStateConflict,
			"Restore source or destination changed before admission; submit a new action",
		)
	})
	conditions, _, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: conditions, Mutations: keyvalue.CloneMutations(mutations)}, nil
}
