package etcd

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type PreparedVolumeRestore struct {
	EnvironmentName string
	HostID          string
	Restore         backupruntime.BackupRestoreRecord
	Scope           *agentpb.BackupPlanScope
	Authority       *agentpb.BackupStepAuthority
	Artifacts       []*agentpb.ComposeArtifact
	Owner           taskjournal.TaskOwner
	Publication     *PreparedVolumeRestorePublication
}

type volumeRestoreAdmissionState struct {
	hostID     string
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
	host, err := hostbindings.ReadLocal(ctx, repository.store, selected.ReadRevision)
	if err != nil {
		return zero, err
	}
	conditions = append(conditions, host.Conditions...)
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
		hostID:     host.Binding.HostID,
		repository: repository, restore: backupruntime.CloneBackupRestoreRecord(restore), owner: selected.Owner,
		scope: proto.CloneOf(
			selected.Scope,
		), authority: proto.CloneOf(authority), artifacts: cloneRestoreArtifacts(selected.Artifacts),
		conditions: conditions, mutations: mutations}}
	return PreparedVolumeRestore{
		EnvironmentName: selected.EnvironmentName,
		HostID:          host.Binding.HostID,
		Restore:         backupruntime.CloneBackupRestoreRecord(restore),
		Scope:           proto.CloneOf(selected.Scope),
		Authority:       proto.CloneOf(authority),
		Artifacts:       cloneRestoreArtifacts(selected.Artifacts),
		Owner:           selected.Owner,
		Publication:     publication,
	}, nil
}

func (publication *PreparedVolumeRestorePublication) Clear() {
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
