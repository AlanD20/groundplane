package blueprintplanning

import (
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EnvironmentBlueprintAttachCandidateInput is one fully resolved Attach that
// supplies the encrypted capture for its native Attach work item.
// Versioned backing records are private compare evidence, not desired state.
type EnvironmentBlueprintAttachCandidateInput struct {
	Record                  attachrecord.Record
	Facts                   *attachrecord.EncryptedFacts
	BackingProject          etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	BackingEnvironment      etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	BackingService          etcdstore.Versioned[servicerecord.ServiceRecord]
	RetainedCredentialOwner *etcdstore.Versioned[attachrecord.Record]
	RetainedGrantTargets    []etcdstore.Versioned[attachrecord.Record]
}

type EnvironmentBlueprintBackupPolicySourceInput struct {
	CandidateID string
	Kind        core.BackupSourceKind
	TargetID    string
}

type EnvironmentBlueprintBackupPolicyInput struct {
	EnvironmentID string
	TaskID        string
	ReadRevision  int64
	Retain        bool
	Enabled       bool
	Frequency     string
	Keep          int64
	Encryption    string
	ConnectorName string
	Sources       []EnvironmentBlueprintBackupPolicySourceInput
	Projection    projectionrecord.EnvironmentComposeProjection
	CreatedAt     time.Time
}

func ValidateBlueprintAttachTaskOwner(task TaskIdentity, intent attachrecord.BlueprintAttachTaskIntent) error {
	if task.ID != intent.TaskID || task.Target != intent.EnvironmentID {
		return errs.New(errs.KindStateConflict, "Blueprint Attach intent has the wrong Task owner")
	}
	return nil
}
