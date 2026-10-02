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
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type PreparedVolumeRestore struct {
	Restore     backupruntime.BackupRestoreRecord
	Scope       *agentpb.BackupPlanScope
	Authority   *agentpb.BackupStepAuthority
	Artifact    *agentpb.ComposeArtifact
	Owner       taskjournal.TaskOwner
	Publication *PreparedVolumeRestorePublication
}

type volumeRestoreAdmissionState struct {
	mu         sync.Mutex
	repository *BackupRuntimeRepository
	restore    backupruntime.BackupRestoreRecord
	owner      taskjournal.TaskOwner
	scope      *agentpb.BackupPlanScope
	authority  *agentpb.BackupStepAuthority
	artifact   *agentpb.ComposeArtifact
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

type PreparedVolumeRestorePublication struct{ state *volumeRestoreAdmissionState }

func (repository *BackupRuntimeRepository) PrepareVolumeRestore(ctx context.Context,
	input backupplanning.VolumeRestoreSelectionInput,
) (PreparedVolumeRestore, error) {
	var zero PreparedVolumeRestore
	if repository == nil || repository.Planner == nil || repository.store == nil {
		return zero, errs.New(errs.KindInternal, "Volume Restore admission is not configured")
	}
	selected, err := repository.PrepareVolumeRestoreSelection(ctx, input)
	if err != nil {
		return zero, err
	}
	if err := repository.validateVolumeRestoreSelectedManifest(ctx, selected.Restore.Point); err != nil {
		return zero, err
	}
	authority, err := backupplanning.BuildVolumeRestoreAuthority(selected)
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
	if err := backupruntime.ValidateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		etcdstore.ClearMutationValues(mutations)
		return zero, err
	}
	publication := &PreparedVolumeRestorePublication{state: &volumeRestoreAdmissionState{
		repository: repository, restore: backupruntime.CloneBackupRestoreRecord(restore), owner: selected.Owner,
		scope: proto.CloneOf(
			selected.Scope,
		), authority: proto.CloneOf(authority), artifact: proto.CloneOf(selected.Artifact),
		conditions: conditions, mutations: mutations}}
	return PreparedVolumeRestore{
		Restore:     backupruntime.CloneBackupRestoreRecord(restore),
		Scope:       proto.CloneOf(selected.Scope),
		Authority:   proto.CloneOf(authority),
		Artifact:    proto.CloneOf(selected.Artifact),
		Owner:       selected.Owner,
		Publication: publication,
	}, nil
}

func (publication *PreparedVolumeRestorePublication) Publish(ctx context.Context, task TaskRecord,
	sealed *agentpb.ExecutionPlan, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if publication == nil || publication.state == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Volume Restore publication is not prepared")
	}
	state := publication.state
	state.mu.Lock()
	if state.repository == nil {
		state.mu.Unlock()
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"Volume Restore publication was already consumed",
		)
	}
	repository, restore, scope, authority, artifact := state.repository, state.restore, state.scope, state.authority, state.artifact
	conditions, mutations := state.conditions, state.mutations
	state.repository, state.scope, state.authority, state.artifact = nil, nil, nil, nil
	state.conditions, state.mutations = nil, nil
	state.mu.Unlock()
	defer etcdstore.ClearMutationValues(mutations)
	if task.Owner != state.owner || task.Actor != taskjournal.TaskActorOperator || sealed == nil ||
		!proto.Equal(sealed.BackupScope, scope) || len(sealed.Steps) != 1 ||
		!proto.Equal(sealed.Steps[0].GetBackupStep(), authority) || len(sealed.Artifacts) != 1 ||
		!proto.Equal(sealed.Artifacts[0], artifact) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Volume Restore publication authority changed",
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
			return backupruntime.ValidateVolumeRestoreExecutionPlan(restore, value)
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

func (publication *PreparedVolumeRestorePublication) Clear() {
	if publication == nil || publication.state == nil {
		return
	}
	state := publication.state
	state.mu.Lock()
	defer state.mu.Unlock()
	etcdstore.ClearMutationValues(state.mutations)
	state.repository, state.scope, state.authority, state.artifact = nil, nil, nil, nil
	state.conditions, state.mutations = nil, nil
	state.restore = backupruntime.BackupRestoreRecord{}
}
