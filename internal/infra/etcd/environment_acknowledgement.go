package etcd

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const environmentBlueprintDeletionBatchSize int64 = 32

func (repository *TaskRepository) prepareEnvironmentCreationAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	readRevision int64,
) (Condition, Mutation, []byte, error) {
	stored, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{environmentKey(task.Target)}, Revision: readRevision,
	})
	if err != nil {
		return Condition{}, Mutation{}, nil, err
	}
	if len(stored.Values) != 1 || stored.Values[0] == nil {
		return Condition{}, Mutation{}, nil, errs.New(errs.KindEnvironmentNotFound, "environment was not found")
	}
	value := stored.Values[0]
	record, err := decodeEnvironment(value.Value)
	if err != nil {
		return Condition{}, Mutation{}, nil, err
	}
	if record.ID != task.Target || record.CreateTaskID != task.ID {
		return Condition{}, Mutation{}, nil, errs.New(
			errs.KindStateConflict,
			"Environment provisioning belongs to another Task",
		)
	}
	replacement, err := CompleteEnvironmentProvisioning(
		record,
		task.ID,
		terminalStatus == TaskStatusCompleted,
	)
	if err != nil {
		return Condition{}, Mutation{}, nil, err
	}
	encoded, err := encodeEnvironment(replacement)
	if err != nil {
		return Condition{}, Mutation{}, nil, err
	}
	return Condition{Key: environmentKey(record.ID), ModRevision: value.ModRevision}, Mutation{
		Type: MutationPut, Key: environmentKey(record.ID), Value: encoded,
	}, encoded, nil
}

func (repository *TaskRepository) validateEnvironmentCreationReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	readRevision int64,
) error {
	stored, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{environmentKey(task.Target)}, Revision: readRevision,
	})
	if err != nil {
		return err
	}
	if len(stored.Values) != 1 || stored.Values[0] == nil {
		return errs.New(errs.KindEnvironmentNotFound, "environment was not found")
	}
	record, err := decodeEnvironment(stored.Values[0].Value)
	if err != nil {
		return err
	}
	want := EnvironmentProvisioningFailed
	if terminalStatus == TaskStatusCompleted {
		want = EnvironmentProvisioningReady
	}
	if record.ID != task.Target || record.CreateTaskID != task.ID || record.ProvisioningState != want {
		return errs.New(errs.KindStateConflict, "Environment provisioning terminal state does not match its Task")
	}
	return nil
}

func (repository *TaskRepository) finalizeEnvironmentBlueprintRevisionBatch(
	ctx context.Context,
	task TaskRecord,
	updatedAt time.Time,
) (bool, error) {
	prefix := environmentBlueprintRevisionsPrefix(task.Target)
	page, err := repository.store.Range(ctx, RangeRequest{
		Prefix: prefix, Limit: environmentBlueprintDeletionBatchSize,
	})
	if err != nil {
		return false, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > int(environmentBlueprintDeletionBatchSize) {
		return false, errs.New(errs.KindInternal, "Environment Blueprint deletion page is invalid")
	}
	if len(page.Values) == 0 {
		return false, nil
	}
	tombstoneResult, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys:     []string{deletionTombstoneKey(string(DeletionTargetEnvironment), task.Target)},
		Revision: page.ReadRevision,
	})
	if err != nil {
		return false, err
	}
	if tombstoneResult == nil || len(tombstoneResult.Values) != 1 || tombstoneResult.Values[0] == nil {
		return false, errs.New(errs.KindStateConflict, "Environment deletion tombstone is missing")
	}
	tombstoneValue := tombstoneResult.Values[0]
	tombstone, err := decodeDeletionTombstone(tombstoneValue.Value)
	if err != nil {
		return false, err
	}
	if tombstone.TargetKind != DeletionTargetEnvironment || tombstone.TargetID != task.Target ||
		tombstone.TaskID != task.ID ||
		(tombstone.Phase != DeletionPhaseHostEffects && tombstone.Phase != DeletionPhaseFinalizing) {
		return false, errs.New(errs.KindStateConflict, "Environment deletion tombstone does not match its Task")
	}
	revisionID, err := environmentBlueprintRevisionIDFromKey(prefix, page.Values[len(page.Values)-1].Key)
	if err != nil {
		return false, err
	}
	tombstone.Phase = DeletionPhaseFinalizing
	tombstone.Checkpoint = DeletionCheckpoint{ResourceKind: "blueprint_revision", StableID: revisionID}
	tombstone.UpdatedAt = updatedAt
	encodedTombstone, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		return false, err
	}
	defer clear(encodedTombstone)

	conditions := make([]Condition, 0, len(page.Values)+1)
	mutations := make([]Mutation, 0, len(page.Values)+1)
	for _, value := range page.Values {
		conditions = append(conditions, Condition{Key: value.Key, ModRevision: value.ModRevision})
		mutations = append(mutations, Mutation{Type: MutationDelete, Key: value.Key})
	}
	conditions = append(conditions, Condition{
		Key:         deletionTombstoneKey(string(DeletionTargetEnvironment), task.Target),
		ModRevision: tombstoneValue.ModRevision,
	})
	mutations = append(mutations, Mutation{
		Type:  MutationPut,
		Key:   deletionTombstoneKey(string(DeletionTargetEnvironment), task.Target),
		Value: encodedTombstone,
	})
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return false, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "Environment Blueprint finalization state changed")
	}
	return true, nil
}

func environmentBlueprintRevisionIDFromKey(prefix string, key string) (string, error) {
	remainder := strings.TrimPrefix(key, prefix)
	separator := strings.IndexByte(remainder, '/')
	if remainder == key || separator <= 0 || validateStableID(ids.KindTask, remainder[:separator]) != nil {
		return "", errs.New(errs.KindInternal, "Environment Blueprint revision key is corrupt")
	}
	return remainder[:separator], nil
}

func (repository *TaskRepository) prepareEnvironmentRemovalAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	readRevision int64,
) ([]Condition, []Mutation, error) {
	stored, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			environmentKey(task.Target),
			deletionTombstoneKey(string(DeletionTargetEnvironment), task.Target),
			environmentBlueprintHeadKey(task.Target),
			environmentComposeProjectionKey(task.Target),
			environmentPoolRegistryKey,
		},
		Revision: readRevision,
	})
	if err != nil {
		return nil, nil, err
	}
	if len(stored.Values) != 5 || stored.Values[0] == nil || stored.Values[1] == nil || stored.Values[4] == nil {
		return nil, nil, errs.New(errs.KindInternal, "Environment deletion state is inconsistent")
	}
	if (stored.Values[2] == nil) != (stored.Values[3] == nil) {
		return nil, nil, errs.New(errs.KindInternal, "Environment Blueprint state is inconsistent")
	}
	environmentValue := stored.Values[0]
	environment, err := decodeEnvironment(environmentValue.Value)
	if err != nil {
		return nil, nil, err
	}
	tombstoneValue := stored.Values[1]
	tombstone, err := decodeDeletionTombstone(tombstoneValue.Value)
	if err != nil {
		return nil, nil, err
	}
	if environment.ID != task.Target || tombstone.TargetKind != DeletionTargetEnvironment ||
		tombstone.TargetID != environment.ID || tombstone.TargetRevision != environmentValue.ModRevision ||
		tombstone.TaskID != task.ID ||
		(tombstone.Phase != DeletionPhaseHostEffects && tombstone.Phase != DeletionPhaseFinalizing) {
		return nil, nil, errs.New(errs.KindStateConflict, "Environment deletion tombstone does not match its Task")
	}
	poolRegistry, err := decodeEnvelope[EnvironmentPoolRegistry](
		stored.Values[4].Value,
		"environment_pool_registry",
	)
	if err != nil || validateEnvironmentPoolRegistry(poolRegistry) != nil ||
		poolRegistry.Reservations[environment.ID] != environment.NetworkPool {
		return nil, nil, errs.New(errs.KindInternal, "Environment pool reservation is inconsistent")
	}
	indexes, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			environmentNameKey(environment.ProjectID, environment.Name),
			environmentOwnerKey(environment.ProjectID, environment.ID),
		},
		Revision: readRevision,
	})
	if err != nil {
		return nil, nil, err
	}
	if len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != environment.ID || string(indexes.Values[1].Value) != environment.ID {
		return nil, nil, errs.New(errs.KindInternal, "Environment deletion indexes are corrupt")
	}
	conditions := []Condition{
		{Key: environmentKey(environment.ID), ModRevision: environmentValue.ModRevision},
		{Key: environmentNameKey(environment.ProjectID, environment.Name), ModRevision: indexes.Values[0].ModRevision},
		{Key: environmentOwnerKey(environment.ProjectID, environment.ID), ModRevision: indexes.Values[1].ModRevision},
		{
			Key:         deletionTombstoneKey(string(DeletionTargetEnvironment), environment.ID),
			ModRevision: tombstoneValue.ModRevision,
		},
		{Key: environmentBlueprintHeadKey(environment.ID), ModRevision: keyValueRevision(stored.Values[2])},
		{Key: environmentComposeProjectionKey(environment.ID), ModRevision: keyValueRevision(stored.Values[3])},
	}
	mutations := []Mutation{{
		Type: MutationDelete, Key: deletionTombstoneKey(string(DeletionTargetEnvironment), environment.ID),
	}}
	if terminalStatus == TaskStatusCompleted {
		nextPoolRegistry, err := poolRegistry.Release(environment.ID, environment.NetworkPool)
		if err != nil {
			return nil, nil, err
		}
		conditions = append(conditions, Condition{
			Key: environmentPoolRegistryKey, ModRevision: stored.Values[4].ModRevision,
		})
		if len(nextPoolRegistry.Reservations) == 0 {
			mutations = append(mutations, Mutation{Type: MutationDelete, Key: environmentPoolRegistryKey})
		} else {
			poolRegistryValue, err := encodeEnvelope("environment_pool_registry", nextPoolRegistry)
			if err != nil {
				return nil, nil, err
			}
			mutations = append(mutations, Mutation{
				Type: MutationPut, Key: environmentPoolRegistryKey, Value: poolRegistryValue,
			})
		}
		remaining, err := repository.store.Range(ctx, RangeRequest{
			Prefix: environmentBlueprintRevisionsPrefix(environment.ID), Limit: 1, Revision: readRevision,
		})
		if err != nil {
			return nil, nil, err
		}
		if remaining == nil || remaining.ReadRevision != readRevision {
			return nil, nil, errs.New(errs.KindInternal, "Environment Blueprint finalization revision changed")
		}
		if len(remaining.Values) != 0 {
			return nil, nil, errs.New(
				errs.KindStateConflict,
				"Environment Blueprint revisions remain during finalization",
			)
		}
		if stored.Values[2] != nil {
			mutations = append(mutations,
				Mutation{Type: MutationDelete, Key: environmentBlueprintHeadKey(environment.ID)},
				Mutation{Type: MutationDelete, Key: environmentComposeProjectionKey(environment.ID)},
			)
		}
		mutations = append(mutations,
			Mutation{Type: MutationDelete, Key: environmentNameKey(environment.ProjectID, environment.Name)},
			Mutation{Type: MutationDelete, Key: environmentOwnerKey(environment.ProjectID, environment.ID)},
			Mutation{Type: MutationDelete, Key: environmentKey(environment.ID)},
		)
	}
	return conditions, mutations, nil
}

func (repository *TaskRepository) validateEnvironmentRemovalReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	readRevision int64,
) error {
	stored, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			environmentKey(task.Target),
			deletionTombstoneKey(string(DeletionTargetEnvironment), task.Target),
			environmentBlueprintHeadKey(task.Target),
			environmentComposeProjectionKey(task.Target),
			environmentPoolRegistryKey,
		},
		Revision: readRevision,
	})
	if err != nil {
		return err
	}
	if len(stored.Values) != 5 || stored.Values[1] != nil || (stored.Values[2] == nil) != (stored.Values[3] == nil) {
		return errs.New(errs.KindStateConflict, "Environment deletion terminal state does not match its Task")
	}
	if terminalStatus == TaskStatusCompleted {
		if stored.Values[0] != nil || stored.Values[2] != nil || stored.Values[3] != nil {
			return errs.New(errs.KindStateConflict, "completed Environment deletion retained its target")
		}
		if stored.Values[4] != nil {
			poolRegistry, err := decodeEnvelope[EnvironmentPoolRegistry](
				stored.Values[4].Value,
				"environment_pool_registry",
			)
			if err != nil || validateEnvironmentPoolRegistry(poolRegistry) != nil {
				return errs.New(errs.KindInternal, "Environment pool registry is inconsistent")
			}
			if _, retained := poolRegistry.Reservations[task.Target]; retained {
				return errs.New(errs.KindStateConflict, "completed Environment deletion retained its pool")
			}
		}
		return nil
	}
	if stored.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "failed Environment deletion lost its target")
	}
	environment, err := decodeEnvironment(stored.Values[0].Value)
	if err != nil {
		return err
	}
	if environment.ID != task.Target {
		return errs.New(errs.KindStateConflict, "failed Environment deletion retained another target")
	}
	if stored.Values[4] == nil {
		return errs.New(errs.KindStateConflict, "failed Environment deletion lost its pool")
	}
	poolRegistry, err := decodeEnvelope[EnvironmentPoolRegistry](
		stored.Values[4].Value,
		"environment_pool_registry",
	)
	if err != nil || validateEnvironmentPoolRegistry(poolRegistry) != nil ||
		poolRegistry.Reservations[environment.ID] != environment.NetworkPool {
		return errs.New(errs.KindStateConflict, "failed Environment deletion changed its pool")
	}
	return nil
}
