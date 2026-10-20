package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareManualBackupRun captures sources and prepares their atomic publication.
func (repository *BackupRuntimeRepository) PrepareManualBackupRun(
	ctx context.Context,
	input backupplanning.ManualBackupRunInput,
	resolveDatabase backupplanning.BackupDatabaseIdentityResolver,
) (PreparedManualBackupRun, error) {
	if repository == nil || repository.store == nil {
		return PreparedManualBackupRun{}, errs.New(errs.KindInternal, "backup runtime repository is not configured")
	}
	sources, err := repository.PrepareManualRunSources(ctx, input, resolveDatabase)
	if err != nil {
		return PreparedManualBackupRun{}, err
	}
	host, err := hostbindings.ReadLocal(ctx, repository.store, sources.ReadRevision)
	if err != nil {
		return PreparedManualBackupRun{}, err
	}
	plan, err := repository.prepareBackupRunPublication(ctx, sources.Run, sources.Lock, sources.ReadRevision)
	if err != nil {
		return PreparedManualBackupRun{}, err
	}
	actor := taskjournal.TaskActorOperator
	if sources.Run.Initiator == backupruntime.BackupRunInitiatorSchedule {
		actor = taskjournal.TaskActorSystem
	}
	initiation, err := newTaskInitiation(sources.Owner, actor)
	if err != nil {
		plan.clear()
		return PreparedManualBackupRun{}, err
	}
	plan.hostID, plan.initiation = host.Binding.HostID, initiation
	plan.conditions = append(plan.conditions, host.Conditions...)
	publication := &PreparedBackupRunPublication{
		state: prepareBackupRunPublicationState(repository, plan, sources.Scope, sources.Authority, sources.Artifacts),
	}
	return PreparedManualBackupRun{
		EnvironmentName: sources.EnvironmentName,
		HostID:          host.Binding.HostID,
		Scope:           sources.Scope, Authority: sources.Authority, Artifacts: sources.Artifacts,
		Run: backupruntime.CloneBackupRunPublicationRecord(sources.Run), Owner: sources.Owner, Publication: publication,
	}, nil
}
