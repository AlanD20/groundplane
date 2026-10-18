package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Backup policy is a Controller effect of the visible Apply, not an Agent
// no-op. Its prepared mutation joins runtime sealing under the same head,
// Task revision and ledger epoch; a retry reads the already-sealed projection.
func (service *Service) prepareAuthoredRuntimeBackup(
	ctx context.Context,
	parent workexecution.Task,
	source authoredRuntimeProjectionSource,
	snapshot blueprintunits.Snapshot,
	projection projectionrecord.EnvironmentComposeProjection,
	attaches []etcdstore.Versioned[attachrecord.Record],
) (blueprintplanning.BlueprintBackupPolicyPreparation, error) {
	if err := requireAuthoredBackupInputs(source.desired.Record, source.identities.Record, snapshot); err != nil {
		return blueprintplanning.BlueprintBackupPolicyPreparation{}, err
	}
	policy, prepared, err := service.prepareEnvironmentBlueprintBackup(
		ctx, parent.Scope.EnvironmentID, parent.ID, snapshot.ReadRevision,
		source.desired.Record.Input.Backup, projection,
		preparedBlueprintAttaches{effective: attaches},
		func(kind ids.Kind, name string) string {
			return ids.DeriveAt(kind, parent.CreatedAt, parent.ID, name)
		},
		service.now().UTC(),
	)
	if err != nil {
		return blueprintplanning.BlueprintBackupPolicyPreparation{}, err
	}
	if policy == nil {
		prepared.Clear()
		return blueprintplanning.BlueprintBackupPolicyPreparation{}, errs.New(
			errs.KindStateConflict, "Blueprint Backup policy preparation is unavailable",
		)
	}
	return prepared, nil
}

// Attach and Entry settlement is required by the common runtime gate. A
// selected Volume must also have an acknowledged ensure before policy activation.
func requireAuthoredBackupInputs(
	desired projectionrecord.EnvironmentDesiredInput,
	identities projectionrecord.EnvironmentOwnedIdentities,
	snapshot blueprintunits.Snapshot,
) error {
	if desired.Input.Backup == nil {
		return nil
	}
	for _, source := range desired.Input.Backup.Sources {
		if source.Kind != core.BackupSourceVolume {
			continue
		}
		var targetID string
		for _, volume := range identities.Volumes {
			if volume.Slug == source.Ref {
				targetID = volume.ID
				break
			}
		}
		if targetID == "" {
			return errs.New(errs.KindStateConflict, "Blueprint Backup Volume identity is unavailable")
		}
		if !authoredRuntimeInputApplied(snapshot, blueprintunits.ResourceKey{
			Kind: ids.KindVolume, ID: targetID,
		}) {
			return errs.New(errs.KindResourceInUse, "Blueprint Backup Volume is not applied yet")
		}
	}
	return nil
}
