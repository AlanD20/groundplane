package backupplanning

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

const (
	// These are durable wire values, kept infra-local to preserve the import matrix.
	backupConnectorKindS3Compatible = "s3-compatible"
	backupAttachStatusReady         = "ready"
)

// ManualBackupRunInput carries new operation identities, one selected revision,
// and Controller-owned resolvers for evidence requiring protected selection.
// The repository derives durable resource and encrypted-slot evidence.
type ManualBackupRunInput struct {
	EnvironmentID      string
	TaskID             string
	OperationID        string
	PlanID             string
	FixedRevision      int64
	CreatedAt          time.Time
	Initiator          backupruntime.BackupRunInitiator
	ScheduledAt        *time.Time
	ResolveConfig      BackupConfigSnapshotResolver
	ResolveServiceFact BackupServiceFactResolver
}

type BackupDatabaseIdentity struct {
	Database string
	Role     string
}

// BackupDatabaseIdentityResolver opens only the fixed-revision encrypted facts
// supplied by the repository.
type BackupDatabaseIdentityResolver func(
	context.Context,
	etcdstore.Versioned[attachrecord.Record],
	attachrecord.EncryptedFacts,
	func(BackupDatabaseIdentity) error,
) error

// BackupConfigSnapshotInput selects the complete Entry/value set at the run's
// fixed revision. Preparation computes complete authority without writes;
// durable capture follows atomic Task/cursor publication and checks that same
// authority. TaskID is the current attempt, SnapshotID its retained original.
type BackupConfigSnapshotInput struct {
	TaskID        string
	SnapshotID    string
	EnvironmentID string
	SourceID      string
	ReadRevision  int64
}

type BackupConfigSnapshotResolver func(context.Context, BackupConfigSnapshotInput) (*agentpb.BackupConfigCaptureAuthority, error)

// BackupServiceFactInput requires desired, captured Compose, runtime intent,
// label and repository evidence from this exact MVCC view. A missing runtime
// witness is not permission to substitute the latest projection or image tag.
type BackupServiceFactInput struct {
	ServiceID     string
	EnvironmentID string
	ReadRevision  int64
}

type BackupServiceFactEvidence struct {
	Fact     *agentpb.BackupServiceFact
	Artifact *agentpb.ComposeArtifact
}

type BackupServiceFactResolver func(context.Context, BackupServiceFactInput) (*BackupServiceFactEvidence, error)
