package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type restorePublication interface {
	Publish(
		context.Context,
		etcd.TaskRecord,
		*agentpb.ExecutionPlan,
		idempotency.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	Clear()
}

type preparedRestore struct {
	Restore     backupruntime.BackupRestoreRecord
	Scope       *agentpb.BackupPlanScope
	Authority   *agentpb.BackupStepAuthority
	Artifact    *agentpb.ComposeArtifact
	Artifacts   []*agentpb.ComposeArtifact
	Owner       taskjournal.TaskOwner
	Publication restorePublication
}

func (service *RestoreService) prepareRestore(ctx context.Context, environmentID, taskID string,
	request apiTypes.RestoreRequest, createdAt time.Time, usesOldIdentity bool,
) (preparedRestore, error) {
	// Choose the source variant at the same fixed view used by its complete
	// selector. Publication then compares the selected source and target records.
	key := backuppolicy.BackupSourceKey(request.SourceID)
	read, err := service.runtime.ReadCurrentKeys(ctx, []string{key})
	if err != nil {
		return preparedRestore{}, err
	}
	defer etcdstore.ClearValues(read.Values)
	if read.ReadRevision <= 0 || len(read.Values) != 1 || read.Values[0] == nil {
		return preparedRestore{}, errs.New(errs.KindStateConflict, "Restore source is unavailable")
	}
	source, err := backuppolicy.DecodeBackupSourceRecord(read.Values[0].Value)
	if err != nil || source.ID != request.SourceID || source.EnvironmentID != environmentID {
		return preparedRestore{}, errs.New(errs.KindStateConflict, "Restore source does not belong to this Environment")
	}
	operationID := ids.New(ids.KindOperation)
	switch backupruntime.BackupRuntimeSourceKind(source.Kind) {
	case backupruntime.BackupRuntimeSourceConfig:
		selected, err := service.runtime.PrepareConfigRestore(ctx, backupplanning.ConfigRestoreSelectionInput{
			EnvironmentID: environmentID, SourceID: request.SourceID, RecoveryPointID: request.RecoveryPointID,
			TaskID: taskID, OperationID: operationID, CreatedAt: createdAt, UsesOldIdentity: usesOldIdentity,
			FixedRevision: read.ReadRevision,
			ResolveFiles: func(ctx context.Context, projection environmentprojection.EnvironmentComposeProjection) (*agentpb.BackupConfigFileContext, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return BuildConfigRestoreFileContext(service.volumeRoot, projection)
			},
		})
		if err != nil {
			return preparedRestore{}, err
		}
		return preparedRestore{Restore: selected.Restore, Scope: selected.Scope, Authority: selected.Authority,
			Owner: selected.Owner, Publication: selected.Publication}, nil
	case backupruntime.BackupRuntimeSourceVolume:
		selected, err := service.runtime.PrepareVolumeRestore(ctx, backupplanning.VolumeRestoreSelectionInput{
			EnvironmentID: environmentID, SourceID: request.SourceID, RecoveryPointID: request.RecoveryPointID,
			TaskID: taskID, OperationID: operationID, CreatedAt: createdAt, UsesOldIdentity: usesOldIdentity,
			FixedRevision: read.ReadRevision, ResolveServiceFact: service.serviceFacts,
		})
		if err != nil {
			return preparedRestore{}, err
		}
		return preparedRestore{Restore: selected.Restore, Scope: selected.Scope, Authority: selected.Authority,
			Artifact: selected.Artifact, Owner: selected.Owner, Publication: selected.Publication}, nil
	case backupruntime.BackupRuntimeSourceAttach:
		selected, err := service.runtime.PreparePostgresRestore(ctx, backupplanning.PostgresRestoreSelectionInput{
			EnvironmentID: environmentID, SourceID: request.SourceID, RecoveryPointID: request.RecoveryPointID,
			TaskID: taskID, OperationID: operationID, CreatedAt: createdAt, UsesOldIdentity: usesOldIdentity,
			FixedRevision: read.ReadRevision, ResolvePostgres: service.resolvePostgres,
			ResolveServiceFact: service.serviceFacts,
		})
		if err != nil {
			return preparedRestore{}, err
		}
		return preparedRestore{Restore: selected.Restore, Scope: selected.Scope, Authority: selected.Authority,
			Artifacts: selected.Artifacts, Owner: selected.Owner, Publication: selected.Publication}, nil
	default:
		return preparedRestore{}, errs.New(errs.KindStrategyNotImplemented, "Restore source execution is not available")
	}
}

func (prepared preparedRestore) buildPlan(task etcd.TaskRecord) (*agentpb.ExecutionPlan, error) {
	switch prepared.Restore.Point.SourceKind {
	case backupruntime.BackupRuntimeSourceConfig:
		return BuildConfigRestorePlan(ConfigRestorePlanInput{Task: task, Restore: prepared.Restore,
			Scope: prepared.Scope, Authority: prepared.Authority})
	case backupruntime.BackupRuntimeSourceVolume:
		return BuildVolumeRestorePlan(VolumeRestorePlanInput{Task: task, Restore: prepared.Restore,
			Scope: prepared.Scope, Authority: prepared.Authority, Artifact: prepared.Artifact})
	case backupruntime.BackupRuntimeSourceAttach:
		return BuildPostgresRestorePlan(PostgresRestorePlanInput{Task: task, Restore: prepared.Restore,
			Scope: prepared.Scope, Authority: prepared.Authority, Artifacts: prepared.Artifacts})
	default:
		return nil, errs.New(errs.KindStrategyNotImplemented, "Restore source execution is not available")
	}
}
