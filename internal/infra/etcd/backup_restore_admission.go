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

type PreparedConfigRestore struct {
	HostID      string
	Restore     backupruntime.BackupRestoreRecord
	Scope       *agentpb.BackupPlanScope
	Authority   *agentpb.BackupStepAuthority
	Owner       taskjournal.TaskOwner
	Publication *PreparedConfigRestorePublication
}

type configRestoreAdmissionState struct {
	hostID     string
	mu         sync.Mutex
	repository *BackupRuntimeRepository
	restore    backupruntime.BackupRestoreRecord
	owner      taskjournal.TaskOwner
	scope      *agentpb.BackupPlanScope
	authority  *agentpb.BackupStepAuthority
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

// PreparedConfigRestorePublication owns a single atomic publication attempt.
// None of selection, managed-file planning or preparation writes live state.
type PreparedConfigRestorePublication struct{ state *configRestoreAdmissionState }

func (repository *BackupRuntimeRepository) PrepareConfigRestore(ctx context.Context,
	input backupplanning.ConfigRestoreSelectionInput,
) (PreparedConfigRestore, error) {
	var zero PreparedConfigRestore
	if repository == nil || repository.Planner == nil || repository.store == nil {
		return zero, errs.New(errs.KindInternal, "config restore admission is not configured")
	}
	selected, err := repository.PrepareConfigRestoreSelection(ctx, input)
	if err != nil {
		return zero, err
	}
	authority, err := backupplanning.BuildConfigRestoreAuthority(selected)
	if err != nil {
		return zero, err
	}
	fence, err := environmentfence.LoadOrdinary(
		ctx,
		repository.store,
		selected.Restore.EnvironmentID,
		selected.ReadRevision,
	)
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
		}, epoch}
	if err := backupruntime.ValidateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		etcdstore.ClearMutationValues(mutations)
		return zero, err
	}
	publication := &PreparedConfigRestorePublication{state: &configRestoreAdmissionState{
		hostID:     host.Binding.HostID,
		repository: repository, restore: backupruntime.CloneBackupRestoreRecord(restore), owner: selected.Owner,
		scope: proto.CloneOf(selected.Scope), authority: proto.CloneOf(authority),
		conditions: conditions, mutations: mutations}}
	return PreparedConfigRestore{Restore: backupruntime.CloneBackupRestoreRecord(restore),
		HostID: host.Binding.HostID,
		Scope: proto.CloneOf(
			selected.Scope,
		), Authority: proto.CloneOf(authority), Owner: selected.Owner, Publication: publication}, nil
}

func (publication *PreparedConfigRestorePublication) Clear() {
	if publication == nil || publication.state == nil {
		return
	}
	state := publication.state
	state.mu.Lock()
	defer state.mu.Unlock()
	etcdstore.ClearMutationValues(state.mutations)
	state.repository, state.scope, state.authority = nil, nil, nil
	state.conditions, state.mutations = nil, nil
	state.restore = backupruntime.BackupRestoreRecord{}
}
