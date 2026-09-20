package etcd

import (
	corebackup "github.com/AlanD20/groundplane/internal/core/backup"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"sync"
	"time"
)

const backupKeyRotationTimeoutSeconds = corebackup.KeyRotationTimeoutSeconds

// BackupKeyRotationInput contains only freshly allocated operation identity.
// The repository derives all hierarchy and key evidence at one MVCC revision.
type BackupKeyRotationInput struct {
	EnvironmentID string
	TaskID        string
	OperationID   string
	PlanID        string
	CreatedAt     time.Time
}

// PreparedBackupKeyRotation is one-shot authority for publishing a rotation.
// Its wrapped identity is safe to retain durably; no plaintext identity is
// present in this value.
type PreparedBackupKeyRotation struct {
	Owner       TaskOwner
	Publication *PreparedBackupKeyRotationPublication
}

// Clear abandons unused prepared authority and erases its wrapped ciphertext.
func (prepared *PreparedBackupKeyRotation) Clear() {
	if prepared == nil {
		return
	}
	prepared.Publication.Clear()
	prepared.Publication = nil
	prepared.Owner = TaskOwner{}
}

type backupKeyRotationPublicationPlan struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	record     backupruntime.BackupKeyRotationRecord
}

type preparedBackupKeyRotationState struct {
	mu         sync.Mutex
	repository *BackupPolicyRepository
	plan       backupKeyRotationPublicationPlan
	consumed   bool
}

// PreparedBackupKeyRotationPublication is one-shot publication authority.
type PreparedBackupKeyRotationPublication struct {
	state *preparedBackupKeyRotationState
}

// Clear abandons a prepared publication and erases its wrapped ciphertext buffer.
func (publication *PreparedBackupKeyRotationPublication) Clear() {
	if publication == nil || publication.state == nil {
		return
	}
	publication.state.mu.Lock()
	defer publication.state.mu.Unlock()
	publication.state.plan.clear()
	publication.state.repository = nil
	publication.state.consumed = true
}
