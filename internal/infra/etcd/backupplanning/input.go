package backupplanning

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"
)

const (
	// These are durable wire values, kept infra-local to preserve the import matrix.
	backupConnectorKindS3Compatible = "s3-compatible"
	backupAttachStatusReady         = "ready"
)

// ManualBackupRunInput contains only newly allocated operation identities and
// an optional caller-selected read revision. The repository derives evidence.
type ManualBackupRunInput struct {
	EnvironmentID string
	TaskID        string
	OperationID   string
	PlanID        string
	FixedRevision int64
	CreatedAt     time.Time
	Initiator     backupruntime.BackupRunInitiator
	ScheduledAt   *time.Time
}

type BackupPostgresIdentity struct {
	Database string
	Role     string
}

// BackupPostgresIdentityResolver opens only the fixed-revision encrypted facts
// supplied by the repository.
type BackupPostgresIdentityResolver func(
	context.Context,
	etcdstore.Versioned[attachrecord.Record],
	attachrecord.EncryptedFacts,
	func(BackupPostgresIdentity) error,
) error
