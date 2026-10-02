package etcd

import (
	"context"
	"sync"
	"time"

	backupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppruneevidence"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type PreparedBackupPrune struct {
	Dispatch    backupruntime.BackupRecoveryPointPruneDispatchRecord
	Scope       *agentpb.BackupPlanScope
	Evidence    []backupplanning.PruneExecutionEvidence
	Owner       taskjournal.TaskOwner
	Publication *PreparedBackupPrunePublication
}

type preparedBackupPruneState struct {
	mu         sync.Mutex
	repository *BackupRuntimeRepository
	plan       backupPruneTransactionPlan
	scope      *agentpb.BackupPlanScope
	owner      taskjournal.TaskOwner
}

type PreparedBackupPrunePublication struct{ state *preparedBackupPruneState }

// PrepareBackupPrune fixes the original operation and ordered tombstones. It
// reads every planning input at one revision; Publish commits the Task,
// procedure, assigned tombstones, dispatch, lock and epoch in one transaction.
func (repository *BackupRuntimeRepository) PrepareBackupPrune(
	ctx context.Context,
	pending []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
	taskID string,
	createdAt time.Time,
) (PreparedBackupPrune, error) {
	var zero PreparedBackupPrune
	if repository == nil || repository.store == nil || len(pending) == 0 ||
		len(pending) > backupruntime.MaximumBackupPruneDispatchPoints {
		return zero, errs.New(errs.KindValidationFailed, "backup prune candidates are invalid")
	}
	first := pending[0].Record
	dispatch := backupruntime.BackupRecoveryPointPruneDispatchRecord{
		TaskID: taskID, OperationID: first.OperationID, EnvironmentID: first.Point.EnvironmentID,
		CreatedAt: createdAt, RecoveryPointIDs: make([]string, len(pending)),
	}
	for index, item := range pending {
		if item.Record.State != backupruntime.BackupPrunePending ||
			item.Record.OperationID != dispatch.OperationID ||
			item.Record.Point.EnvironmentID != dispatch.EnvironmentID ||
			item.Record.PolicyRevision != first.PolicyRevision ||
			item.Record.PolicySHA256 != first.PolicySHA256 {
			return zero, errs.New(errs.KindValidationFailed, "backup prune candidates have different authority")
		}
		dispatch.RecoveryPointIDs[index] = item.Record.Point.ID
	}
	if err := backupruntime.ValidateBackupRecoveryPointPruneDispatchRecord(dispatch); err != nil {
		return zero, err
	}
	lock := backupruntime.BackupOperationLockRecord{
		EnvironmentID: dispatch.EnvironmentID, OperationID: dispatch.OperationID,
		TaskID: taskID, Kind: backupruntime.BackupOperationPrune,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	plan, err := repository.prepareBackupPrunePublication(ctx, pending, dispatch, lock)
	if err != nil {
		return zero, err
	}
	release := true
	defer func() {
		if release {
			plan.clear()
		}
	}()
	read, err := repository.ReadFixedKeys(
		ctx,
		[]string{hierarchyrecord.EnvironmentKey(dispatch.EnvironmentID)},
		plan.readRevision,
	)
	if err != nil {
		return zero, err
	}
	defer etcdstore.ClearValues(read.Values)
	if len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].ModRevision != plan.evidence[0].EnvironmentRevision {
		return zero, errs.New(errs.KindStateConflict, "backup prune Environment changed")
	}
	environment, err := hierarchyrecord.DecodeEnvironment(read.Values[0].Value)
	if err != nil || environment.ID != dispatch.EnvironmentID {
		return zero, backupruntime.CorruptBackupRuntimeRecord()
	}
	ancestry, err := repository.ReadFixedKeys(
		ctx,
		[]string{hierarchyrecord.ProjectKey(environment.ProjectID)},
		plan.readRevision,
	)
	if err != nil {
		return zero, err
	}
	defer etcdstore.ClearValues(ancestry.Values)
	if len(ancestry.Values) != 1 || ancestry.Values[0] == nil {
		return zero, errs.New(errs.KindStateConflict, "backup prune Project is unavailable")
	}
	project, err := hierarchyrecord.DecodeProject(ancestry.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID || project.Kind != hierarchyrecord.ProjectKindTenant {
		return zero, backupruntime.CorruptBackupRuntimeRecord()
	}
	tenant, err := repository.ReadFixedKeys(
		ctx,
		[]string{hierarchyrecord.TenantKey(project.TenantID)},
		plan.readRevision,
	)
	if err != nil {
		return zero, err
	}
	defer etcdstore.ClearValues(tenant.Values)
	if len(tenant.Values) != 1 || tenant.Values[0] == nil {
		return zero, errs.New(errs.KindStateConflict, "backup prune Tenant is unavailable")
	}
	tenantRecord, err := hierarchyrecord.DecodeTenant(tenant.Values[0].Value)
	if err != nil || tenantRecord.ID != project.TenantID {
		return zero, backupruntime.CorruptBackupRuntimeRecord()
	}
	owner, err := taskjournal.EnvironmentTaskOwner(project, environment)
	if err != nil {
		return zero, err
	}
	scope := &agentpb.BackupPlanScope{
		ProjectId: project.ID, Project: backuppruneevidence.RevisionDigest(ancestry.Values[0]),
		EnvironmentId: environment.ID, Environment: backuppruneevidence.RevisionDigest(read.Values[0]),
		TaskAttempt: 1,
	}
	prepared := PreparedBackupPrune{
		Dispatch: dispatch, Scope: proto.CloneOf(scope), Evidence: cloneBackupPruneExecutionEvidence(plan.evidence),
		Owner: owner, Publication: &PreparedBackupPrunePublication{state: &preparedBackupPruneState{
			repository: repository, plan: plan, scope: scope, owner: owner,
		}},
	}
	release = false
	return prepared, nil
}

func (publication *PreparedBackupPrunePublication) Publish(
	ctx context.Context, task TaskRecord, sealed *agentpb.ExecutionPlan,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if publication == nil || publication.state == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "backup prune publication is not prepared")
	}
	state := publication.state
	state.mu.Lock()
	if state.repository == nil {
		state.mu.Unlock()
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "backup prune publication was consumed")
	}
	repository, plan, scope, owner := state.repository, state.plan, state.scope, state.owner
	state.repository, state.plan, state.scope, state.owner = nil, backupPruneTransactionPlan{}, nil, taskjournal.TaskOwner{}
	state.mu.Unlock()
	defer plan.clear()
	if task.Actor != taskjournal.TaskActorSystem || task.Owner != owner ||
		sealed == nil || !proto.Equal(sealed.BackupScope, scope) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "backup prune scope changed")
	}
	initiation, err := newTaskInitiation(task.Owner, task.Actor)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	mutationPlan, err := plan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, mutationPlan)
}

func (publication *PreparedBackupPrunePublication) Clear() {
	if publication == nil || publication.state == nil {
		return
	}
	state := publication.state
	state.mu.Lock()
	defer state.mu.Unlock()
	state.plan.clear()
	state.plan = backupPruneTransactionPlan{}
	state.repository, state.scope, state.owner = nil, nil, taskjournal.TaskOwner{}
}

func cloneBackupPruneExecutionEvidence(
	source []backupplanning.PruneExecutionEvidence,
) []backupplanning.PruneExecutionEvidence {
	copyOfEvidence := make([]backupplanning.PruneExecutionEvidence, len(source))
	for index, item := range source {
		item.RetentionPolicy = proto.CloneOf(item.RetentionPolicy)
		item.ConnectorAuthority = proto.CloneOf(item.ConnectorAuthority)
		item.PointSHA256 = append([]byte(nil), item.PointSHA256...)
		copyOfEvidence[index] = item
	}
	return copyOfEvidence
}
