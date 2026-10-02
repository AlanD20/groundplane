package etcd

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/postgresbackingguard"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type PreparedPostgresRestore struct {
	Restore     backupruntime.BackupRestoreRecord
	Scope       *agentpb.BackupPlanScope
	Authority   *agentpb.BackupStepAuthority
	Artifacts   []*agentpb.ComposeArtifact
	Owner       taskjournal.TaskOwner
	Publication *PreparedPostgresRestorePublication
}

type postgresRestoreAdmissionState struct {
	mu         sync.Mutex
	repository *BackupRuntimeRepository
	restore    backupruntime.BackupRestoreRecord
	owner      taskjournal.TaskOwner
	scope      *agentpb.BackupPlanScope
	authority  *agentpb.BackupStepAuthority
	artifacts  []*agentpb.ComposeArtifact
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

type PreparedPostgresRestorePublication struct {
	state *postgresRestoreAdmissionState
}

func (repository *BackupRuntimeRepository) PreparePostgresRestore(ctx context.Context,
	input backupplanning.PostgresRestoreSelectionInput,
) (PreparedPostgresRestore, error) {
	var zero PreparedPostgresRestore
	if repository == nil || repository.Planner == nil || repository.store == nil {
		return zero, errs.New(errs.KindInternal, "PostgreSQL Restore admission is not configured")
	}
	selected, err := repository.PreparePostgresRestoreSelection(ctx, input)
	if err != nil {
		return zero, err
	}
	authority, err := backupplanning.BuildPostgresRestoreAuthority(selected)
	if err != nil {
		return zero, err
	}
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store,
		selected.Restore.EnvironmentID, selected.ReadRevision)
	if err != nil {
		return zero, err
	}
	conditions, err := environmentfence.AppendConditions(selected.Conditions, fence)
	if err != nil {
		return zero, err
	}
	restore := selected.Restore
	backingEnvironmentID, postgres, err := backupruntime.PostgresRestoreBackingEnvironmentID(restore)
	if err != nil || !postgres {
		if err != nil {
			return zero, err
		}
		return zero, errs.New(errs.KindInternal, "PostgreSQL Restore backing Environment is missing")
	}
	backingGuards, err := postgresbackingguard.PrepareAcquisition(
		ctx,
		repository.store,
		[]string{backingEnvironmentID},
		restore.EnvironmentID,
		postgresbackingguard.Owner(
			backupruntime.BackupOperationRestore,
			restore.OperationID,
			restore.TaskID,
		),
		restore.CreatedAt,
		selected.ReadRevision,
	)
	if err != nil {
		return zero, err
	}
	defer backingGuards.Clear()
	conditions = append(conditions, backingGuards.Conditions...)
	membership, err := backupruntime.BackupRestoreEnvironmentIndexKey(restore.EnvironmentID, restore.TaskID)
	if err != nil {
		return zero, err
	}
	value, err := backupruntime.EncodeBackupRestoreRecord(restore)
	if err != nil {
		return zero, err
	}
	lock := backupruntime.BackupOperationLockRecord{EnvironmentID: restore.EnvironmentID,
		OperationID: restore.OperationID, TaskID: restore.TaskID, Kind: backupruntime.BackupOperationRestore,
		CreatedAt: restore.CreatedAt, UpdatedAt: restore.CreatedAt}
	lockValue, err := backupruntime.EncodeBackupOperationLockRecord(lock)
	if err != nil {
		clear(value)
		return zero, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		clear(value)
		clear(lockValue)
		return zero, err
	}
	conditions = append(conditions, etcdstore.Condition{Key: backupruntime.BackupRestoreKey(restore.TaskID)},
		etcdstore.Condition{Key: membership})
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: backupruntime.BackupRestoreKey(restore.TaskID), Value: value},
		{Type: etcdstore.MutationPut, Key: membership, Value: []byte(restore.TaskID)},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchy.EnvironmentOperationLockKey(restore.EnvironmentID),
			Value: lockValue,
		},
		epoch,
	}
	for _, mutation := range backingGuards.Mutations {
		copyOfMutation := mutation
		copyOfMutation.Value = append([]byte(nil), mutation.Value...)
		mutations = append(mutations, copyOfMutation)
	}
	if err := backupruntime.ValidateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		etcdstore.ClearMutationValues(mutations)
		return zero, err
	}
	artifacts := clonePostgresRestoreArtifacts(selected.Artifacts)
	publication := &PreparedPostgresRestorePublication{state: &postgresRestoreAdmissionState{
		repository: repository, restore: backupruntime.CloneBackupRestoreRecord(restore), owner: selected.Owner,
		scope: proto.CloneOf(
			selected.Scope,
		), authority: proto.CloneOf(authority), artifacts: clonePostgresRestoreArtifacts(artifacts),
		conditions: conditions, mutations: mutations}}
	return PreparedPostgresRestore{
		Restore:     backupruntime.CloneBackupRestoreRecord(restore),
		Scope:       proto.CloneOf(selected.Scope),
		Authority:   proto.CloneOf(authority),
		Artifacts:   artifacts,
		Owner:       selected.Owner,
		Publication: publication,
	}, nil
}

func (publication *PreparedPostgresRestorePublication) Publish(ctx context.Context, task TaskRecord,
	sealed *agentpb.ExecutionPlan, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if publication == nil || publication.state == nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"PostgreSQL Restore publication is not prepared",
		)
	}
	state := publication.state
	state.mu.Lock()
	if state.repository == nil {
		state.mu.Unlock()
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"PostgreSQL Restore publication was already consumed",
		)
	}
	repository, restore, scope, authority, artifacts := state.repository, state.restore, state.scope, state.authority, state.artifacts
	conditions, mutations := state.conditions, state.mutations
	state.repository, state.scope, state.authority, state.artifacts = nil, nil, nil, nil
	state.conditions, state.mutations = nil, nil
	state.mu.Unlock()
	defer etcdstore.ClearMutationValues(mutations)
	if task.Owner != state.owner || task.Actor != taskjournal.TaskActorOperator || sealed == nil ||
		!proto.Equal(sealed.BackupScope, scope) || len(sealed.Steps) != 1 ||
		!proto.Equal(sealed.Steps[0].GetBackupStep(), authority) ||
		!equalPostgresRestoreArtifacts(sealed.Artifacts, artifacts) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"PostgreSQL Restore publication authority changed",
		)
	}
	initiation, err := newTaskInitiation(task.Owner, task.Actor)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := prepareBackupTaskIdempotencyPlan(backupTaskPublicationAuthority{
		taskID: restore.TaskID, operationID: restore.OperationID, environmentID: restore.EnvironmentID,
		taskType: taskjournal.TaskRestore, createdAt: restore.CreatedAt,
		validatePlan: func(value *agentpb.ExecutionPlan) error {
			return backupruntime.ValidatePostgresRestoreExecutionPlan(restore, value)
		}}, conditions, mutations, task, sealed, marker, initiation)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := plan.enforceExistingReplay(repository.validateExistingRestorePublication); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotencyRepository, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotencyRepository.Apply(ctx, marker, plan)
}

func (publication *PreparedPostgresRestorePublication) Clear() {
	if publication == nil || publication.state == nil {
		return
	}
	state := publication.state
	state.mu.Lock()
	defer state.mu.Unlock()
	etcdstore.ClearMutationValues(state.mutations)
	state.repository, state.scope, state.authority, state.artifacts = nil, nil, nil, nil
	state.conditions, state.mutations = nil, nil
	state.restore = backupruntime.BackupRestoreRecord{}
}

func clonePostgresRestoreArtifacts(values []*agentpb.ComposeArtifact) []*agentpb.ComposeArtifact {
	cloned := make([]*agentpb.ComposeArtifact, len(values))
	for index, value := range values {
		cloned[index] = proto.CloneOf(value)
	}
	return cloned
}

func equalPostgresRestoreArtifacts(left, right []*agentpb.ComposeArtifact) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !proto.Equal(left[index], right[index]) {
			return false
		}
	}
	return true
}
