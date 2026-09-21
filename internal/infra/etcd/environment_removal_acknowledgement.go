package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareEnvironmentRemovalAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	readRevision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if terminalStatus == taskjournal.TaskStatusCompleted {
		if err := cleanupTaskEnvironmentDeletionScriptLocators(ctx, repository.store, task.Target, task.ID); err != nil {
			return nil, nil, err
		}
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			hierarchyrecord.EnvironmentKey(task.Target),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), task.Target),
			blueprints.EnvironmentBlueprintHeadKey(task.Target),
			projectionrecord.EnvironmentComposeProjectionStorageKey(task.Target),
			networkreservations.EnvironmentPoolRegistryKey,
			hierarchyrecord.EnvironmentMutationEpochKey(task.Target),
			hierarchyrecord.EnvironmentOperationLockKey(task.Target),
			releaseGroupCollectionEpochKey(task.Target),
		},
		Revision: readRevision,
	})
	if err != nil {
		return nil, nil, err
	}
	if stored == nil || stored.ReadRevision != readRevision || len(stored.Values) != 8 ||
		stored.Values[0] == nil || stored.Values[1] == nil || stored.Values[4] == nil ||
		stored.Values[5] == nil || stored.Values[6] == nil {
		return nil, nil, errs.New(errs.KindInternal, "environment deletion state is inconsistent")
	}
	if (stored.Values[2] == nil) != (stored.Values[3] == nil) {
		return nil, nil, errs.New(errs.KindInternal, "environment Blueprint state is inconsistent")
	}
	environmentValue := stored.Values[0]
	environment, err := hierarchyrecord.DecodeEnvironment(environmentValue.Value)
	if err != nil {
		return nil, nil, err
	}
	tombstoneValue := stored.Values[1]
	tombstone, err := deletionrecord.DecodeDeletionTombstone(tombstoneValue.Value)
	if err != nil {
		return nil, nil, err
	}
	if environment.ID != task.Target || tombstone.TargetKind != deletionrecord.DeletionTargetEnvironment ||
		tombstone.TargetID != environment.ID || tombstone.TargetRevision != environmentValue.ModRevision ||
		tombstone.TaskID != task.ID ||
		(tombstone.Phase != deletionrecord.DeletionPhaseHostEffects && tombstone.Phase != deletionrecord.DeletionPhaseFinalizing) {
		return nil, nil, errs.New(
			errs.KindStateConflict,
			"environment deletion tombstone does not match its Task",
		)
	}
	intentConditions, intentMutations, err := repository.prepareEnvironmentDeletionIntentTerminal(
		ctx, task, tombstone, terminalStatus, readRevision,
	)
	if err != nil {
		return nil, nil, err
	}
	epoch, err := decodeEnvironmentDeletionEpoch(stored.Values[5], environment.ID)
	if err != nil {
		return nil, nil, err
	}
	if _, err := decodeOwnedEnvironmentDeletionLock(stored.Values[6], task); err != nil {
		return nil, nil, err
	}
	ownedFence, err := environmentfence.LoadOwned(
		ctx,
		repository.store,
		environment.ID,
		readRevision,
		environmentfence.Owner{
			Kind: backupruntime.BackupOperationDeletion, OperationID: task.OperationID, TaskID: task.ID,
		},
	)
	if err != nil {
		return nil, nil, err
	}
	poolRegistry, err := recordcodec.Decode[networkreservations.EnvironmentPoolRegistry](
		stored.Values[4].Value,
		"environment_pool_registry",
	)
	if err != nil || networkreservations.ValidateEnvironmentPoolRegistry(poolRegistry) != nil ||
		poolRegistry.Reservations[environment.ID] != environment.NetworkPool {
		return nil, nil, errs.New(errs.KindInternal, "environment pool reservation is inconsistent")
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			hierarchyrecord.EnvironmentNameKey(environment.ProjectID, environment.Name),
			hierarchyrecord.EnvironmentOwnerKey(environment.ProjectID, environment.ID),
		},
		Revision: readRevision,
	})
	if err != nil {
		return nil, nil, err
	}
	if len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(
			indexes.Values[0].Value,
		) != environment.ID || string(indexes.Values[1].Value) != environment.ID {
		return nil, nil, errs.New(errs.KindInternal, "environment deletion indexes are corrupt")
	}
	conditions := []etcdstore.Condition{
		{Key: hierarchyrecord.EnvironmentKey(environment.ID), ModRevision: environmentValue.ModRevision},
		{Key: hierarchyrecord.EnvironmentNameKey(environment.ProjectID, environment.Name), ModRevision: indexes.Values[0].ModRevision},
		{Key: hierarchyrecord.EnvironmentOwnerKey(environment.ProjectID, environment.ID), ModRevision: indexes.Values[1].ModRevision},
		{
			Key:         deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environment.ID),
			ModRevision: tombstoneValue.ModRevision,
		},
		{Key: blueprints.EnvironmentBlueprintHeadKey(environment.ID), ModRevision: keyValueRevision(stored.Values[2])},
		{Key: projectionrecord.EnvironmentComposeProjectionStorageKey(environment.ID), ModRevision: keyValueRevision(stored.Values[3])},
		{Key: hierarchyrecord.EnvironmentMutationEpochKey(environment.ID), ModRevision: stored.Values[5].ModRevision},
		{Key: hierarchyrecord.EnvironmentOperationLockKey(environment.ID), ModRevision: stored.Values[6].ModRevision},
		{Key: releaseGroupCollectionEpochKey(environment.ID), ModRevision: keyValueRevision(stored.Values[7])},
	}
	conditions, err = environmentfence.AppendConditions(conditions, ownedFence)
	if err != nil {
		return nil, nil, err
	}
	conditions = append(conditions, intentConditions...)
	if terminalStatus == taskjournal.TaskStatusCompleted {
		projectedKeys := make([]string, 0)
		if stored.Values[2] != nil || stored.Values[3] != nil {
			if stored.Values[2] == nil || stored.Values[3] == nil {
				return nil, nil, errs.New(errs.KindInternal, "environment deletion desired projection is incomplete")
			}
			revisionID, decodeErr := idempotencyrecord.DecodeTaskReference(stored.Values[2].Value)
			projection, projectionErr := projectionrecord.DecodeEnvironmentComposeProjectionStorage(stored.Values[3].Value)
			if decodeErr != nil || projectionErr != nil || projection.EnvironmentID != environment.ID ||
				projection.RevisionID != revisionID {
				return nil, nil, errs.New(errs.KindInternal, "environment deletion desired projection is corrupt")
			}
			for _, service := range projection.DesiredServices {
				projectedKeys = append(projectedKeys, servicerecord.ServiceRuntimeKey(service.Desired.ID))
			}
			for _, zone := range projection.DesiredZones {
				projectedKeys = append(projectedKeys,
					deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetZone), zone.Desired.ID))
			}
		}
		if err := requireEnvironmentDeletionLiveAuthorityEmpty(
			ctx, repository.store, environment.ID, task.OperationID, readRevision, projectedKeys...,
		); err != nil {
			return nil, nil, err
		}
		conditions = append(conditions,
			environmentDeletionLiveAuthorityConditions(environment.ID, task.OperationID, projectedKeys...)...)
	}
	mutations := make([]etcdstore.Mutation, 0, len(intentMutations)+12)
	if terminalStatus == taskjournal.TaskStatusCompleted {
		mutations = append(mutations,
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environment.ID),
			},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: hierarchyrecord.EnvironmentOperationLockKey(environment.ID)},
		)
		mutations = append(mutations, intentMutations...)
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: hierarchyrecord.EnvironmentMutationEpochKey(environment.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: releaseGroupCollectionEpochKey(environment.ID)},
		)
		nextPoolRegistry, err := poolRegistry.Release(environment.ID, environment.NetworkPool)
		if err != nil {
			return nil, nil, err
		}
		conditions = append(conditions, etcdstore.Condition{
			Key: networkreservations.EnvironmentPoolRegistryKey, ModRevision: stored.Values[4].ModRevision,
		})
		if len(nextPoolRegistry.Reservations) == 0 {
			mutations = append(
				mutations,
				etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: networkreservations.EnvironmentPoolRegistryKey},
			)
		} else {
			poolRegistryValue, err := recordcodec.Encode("environment_pool_registry", nextPoolRegistry)
			if err != nil {
				return nil, nil, err
			}
			mutations = append(mutations, etcdstore.Mutation{
				Type: etcdstore.MutationPut, Key: networkreservations.EnvironmentPoolRegistryKey, Value: poolRegistryValue,
			})
		}
		remaining, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: blueprints.EnvironmentBlueprintRevisionsPrefix(
				environment.ID,
			),
			Limit:    1,
			Revision: readRevision,
		})
		if err != nil {
			return nil, nil, err
		}
		if remaining == nil || remaining.ReadRevision != readRevision {
			return nil, nil, errs.New(
				errs.KindInternal,
				"environment Blueprint finalization revision changed",
			)
		}
		if len(remaining.Values) != 0 {
			return nil, nil, errs.New(
				errs.KindStateConflict,
				"environment Blueprint revisions remain during finalization",
			)
		}
		if stored.Values[2] != nil {
			mutations = append(
				mutations,
				etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: blueprints.EnvironmentBlueprintHeadKey(environment.ID)},
				etcdstore.Mutation{
					Type: etcdstore.MutationDelete,
					Key:  projectionrecord.EnvironmentComposeProjectionStorageKey(environment.ID),
				},
			)
		}
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  hierarchyrecord.EnvironmentNameKey(environment.ProjectID, environment.Name),
			},
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  hierarchyrecord.EnvironmentOwnerKey(environment.ProjectID, environment.ID),
			},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: hierarchyrecord.EnvironmentKey(environment.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: scriptrecord.ScriptSetEnvironmentPrefix(environment.ID), Prefix: true},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: scriptrecord.ScriptEnvironmentLocatorPrefixFor(environment.ID), Prefix: true},
		)
	} else {
		epochValue, err := backupruntime.EncodeEnvironmentMutationEpochRecord(epoch)
		if err != nil {
			return nil, nil, err
		}
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentMutationEpochKey(environment.ID), Value: epochValue,
		})
	}
	if err := environmentfence.ValidateTransactionBudget(conditions, mutations); err != nil {
		clearMutationValues(mutations)
		return nil, nil, err
	}
	return conditions, mutations, nil
}
