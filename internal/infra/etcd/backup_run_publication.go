package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"sync"
)

type PreparedManualBackupRun struct {
	Scope       *agentpb.BackupPlanScope
	Authority   []*agentpb.BackupStepAuthority
	Artifacts   []*agentpb.ComposeArtifact
	Run         backupruntime.BackupRunRecord
	Owner       taskjournal.TaskOwner
	Publication *PreparedBackupRunPublication
}

type preparedBackupRunState struct {
	mu         sync.Mutex
	repository *BackupRuntimeRepository
	plan       backupRunPublicationPlan
	scope      *agentpb.BackupPlanScope
	authority  []*agentpb.BackupStepAuthority
	artifacts  []*agentpb.ComposeArtifact
	consumed   bool
}

// PreparedBackupRunPublication is shared-state one-shot authority.
type PreparedBackupRunPublication struct{ state *preparedBackupRunState }

func prepareBackupRunPublicationState(repository *BackupRuntimeRepository, plan backupRunPublicationPlan,
	scope *agentpb.BackupPlanScope, authority []*agentpb.BackupStepAuthority, artifacts []*agentpb.ComposeArtifact,
) *preparedBackupRunState {
	owned := make([]*agentpb.BackupStepAuthority, len(authority))
	for index, step := range authority {
		owned[index] = proto.CloneOf(step)
	}
	ownedArtifacts := make([]*agentpb.ComposeArtifact, len(artifacts))
	for index, artifact := range artifacts {
		ownedArtifacts[index] = proto.CloneOf(artifact)
	}
	return &preparedBackupRunState{repository: repository, plan: plan, scope: proto.CloneOf(scope), authority: owned,
		artifacts: ownedArtifacts}
}

func (publication *PreparedBackupRunPublication) Record() backupruntime.BackupRunRecord {
	if publication == nil || publication.state == nil {
		return backupruntime.BackupRunRecord{}
	}
	publication.state.mu.Lock()
	defer publication.state.mu.Unlock()
	return backupruntime.CloneBackupRunPublicationRecord(publication.state.plan.record)
}

func (publication *PreparedBackupRunPublication) Publish(
	ctx context.Context,
	task TaskRecord,
	sealed *agentpb.ExecutionPlan,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if publication == nil || publication.state == nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"backup run publication is not prepared",
		)
	}
	publication.state.mu.Lock()
	if publication.state.consumed || publication.state.repository == nil {
		publication.state.mu.Unlock()
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict, "backup run publication was already consumed",
		)
	}
	publication.state.consumed = true
	repository := publication.state.repository
	plan := publication.state.plan
	scope, authority, artifacts := publication.state.scope, publication.state.authority, publication.state.artifacts
	publication.state.repository = nil
	publication.state.plan = backupRunPublicationPlan{}
	publication.state.scope, publication.state.authority, publication.state.artifacts = nil, nil, nil
	publication.state.mu.Unlock()
	defer plan.clear()
	if sealed == nil || scope == nil || !proto.Equal(sealed.BackupScope, scope) ||
		len(sealed.Steps) != len(authority) ||
		len(sealed.Artifacts) != len(artifacts) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"backup publication snapshot authority changed",
		)
	}
	for index, artifact := range artifacts {
		if !proto.Equal(sealed.Artifacts[index], artifact) {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"backup Volume artifact authority changed",
			)
		}
	}
	for index, step := range sealed.Steps {
		if step == nil || !proto.Equal(step.GetBackupStep(), authority[index]) {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"backup publication procedure authority changed",
			)
		}
	}
	if task.Actor != taskjournal.TaskActorOperator && task.Actor != taskjournal.TaskActorSystem {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "backup run task actor must be operator or system",
		)
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

func (publication *PreparedBackupRunPublication) Clear() {
	if publication == nil || publication.state == nil {
		return
	}
	publication.state.mu.Lock()
	defer publication.state.mu.Unlock()
	publication.state.plan.clear()
	publication.state.plan = backupRunPublicationPlan{}
	publication.state.repository = nil
	publication.state.scope, publication.state.authority, publication.state.artifacts = nil, nil, nil
	publication.state.consumed = true
}
