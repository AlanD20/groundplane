package etcd

import (
	"sync"

	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type PreparedManualBackupRun struct {
	EnvironmentName string
	HostID          string
	Scope           *agentpb.BackupPlanScope
	Authority       []*agentpb.BackupStepAuthority
	Artifacts       []*agentpb.ComposeArtifact
	Run             backupruntime.BackupRunRecord
	Owner           taskjournal.TaskOwner
	Publication     *PreparedBackupRunPublication
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
