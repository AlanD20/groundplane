package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BackupTaskAcceptance contains ancestry only. Accepting a queued action must
// not read the current policy, key material, Connector, Point or runtime.
type BackupTaskAcceptance struct {
	Owner taskjournal.TaskOwner
	Name  string
}

func (repository *BackupRuntimeRepository) ReadBackupTaskAcceptance(
	ctx context.Context,
	environmentID string,
) (BackupTaskAcceptance, error) {
	return readBackupTaskAcceptance(ctx, repository.store, environmentID)
}

func (repository *BackupPolicyRepository) ReadBackupTaskAcceptance(
	ctx context.Context,
	environmentID string,
) (BackupTaskAcceptance, error) {
	return readBackupTaskAcceptance(ctx, repository.store, environmentID)
}

func readBackupTaskAcceptance(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
) (BackupTaskAcceptance, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return BackupTaskAcceptance{}, errs.New(errs.KindValidationFailed, "Backup Environment id is invalid")
	}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{hierarchy.EnvironmentKey(environmentID)}})
	if err != nil {
		return BackupTaskAcceptance{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 || read.Values[0] == nil {
		return BackupTaskAcceptance{}, errs.New(errs.KindEnvironmentNotFound, "Backup Environment was not found")
	}
	defer keyvalue.ClearValues(read.Values)
	environment, err := hierarchy.DecodeEnvironment(read.Values[0].Value)
	if err != nil || environment.ID != environmentID {
		return BackupTaskAcceptance{}, errs.New(errs.KindInternal, "Backup Environment identity is corrupt")
	}
	projectRead, err := store.GetMany(
		ctx,
		keyvalue.GetManyRequest{
			Keys:     []string{hierarchy.ProjectKey(environment.ProjectID)},
			Revision: read.ReadRevision,
		},
	)
	if err != nil {
		return BackupTaskAcceptance{}, err
	}
	if projectRead == nil || projectRead.ReadRevision != read.ReadRevision || len(projectRead.Values) != 1 ||
		projectRead.Values[0] == nil {
		return BackupTaskAcceptance{}, errs.New(errs.KindProjectNotFound, "Backup Project was not found")
	}
	defer keyvalue.ClearValues(projectRead.Values)
	project, err := hierarchy.DecodeProject(projectRead.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID || project.Kind != hierarchy.ProjectKindTenant {
		return BackupTaskAcceptance{}, errs.New(
			errs.KindValidationFailed,
			"Backing Environments cannot own Backup actions",
		)
	}
	owner, err := taskjournal.EnvironmentTaskOwner(project, environment)
	return BackupTaskAcceptance{Owner: owner, Name: environment.Name}, err
}
