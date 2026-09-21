package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) validateEnvironmentRemovalReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	readRevision int64,
) error {
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			hierarchyrecord.EnvironmentKey(task.Target),
			deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), task.Target),
			blueprints.EnvironmentBlueprintHeadKey(task.Target),
			projectionrecord.EnvironmentComposeProjectionStorageKey(task.Target),
			environmentPoolRegistryKey,
			hierarchyrecord.EnvironmentMutationEpochKey(task.Target),
			hierarchyrecord.EnvironmentOperationLockKey(task.Target),
			releaseGroupCollectionEpochKey(task.Target),
		},
		Revision: readRevision,
	})
	if err != nil {
		return err
	}
	if stored == nil || stored.ReadRevision != readRevision || len(stored.Values) != 8 ||
		(stored.Values[2] == nil) != (stored.Values[3] == nil) {
		return errs.New(
			errs.KindStateConflict,
			"environment deletion terminal state does not match its Task",
		)
	}
	if terminalStatus == taskjournal.TaskStatusCompleted {
		if stored.Values[0] != nil || stored.Values[1] != nil || stored.Values[2] != nil ||
			stored.Values[3] != nil ||
			stored.Values[5] != nil ||
			stored.Values[6] != nil ||
			stored.Values[7] != nil {
			return errs.New(
				errs.KindStateConflict,
				"completed environment deletion retained its target",
			)
		}
		if err := repository.validateEnvironmentDeletionIntentReplay(
			ctx, task, nil, terminalStatus, readRevision,
		); err != nil {
			return err
		}
		if stored.Values[4] != nil {
			poolRegistry, err := recordcodec.Decode[EnvironmentPoolRegistry](
				stored.Values[4].Value,
				"environment_pool_registry",
			)
			if err != nil || validateEnvironmentPoolRegistry(poolRegistry) != nil {
				return errs.New(errs.KindInternal, "environment pool registry is inconsistent")
			}
			if _, retained := poolRegistry.Reservations[task.Target]; retained {
				return errs.New(
					errs.KindStateConflict,
					"completed environment deletion retained its pool",
				)
			}
		}
		return nil
	}
	if stored.Values[0] == nil || stored.Values[1] == nil || stored.Values[5] == nil ||
		stored.Values[6] == nil {
		return errs.New(errs.KindStateConflict, "environment deletion retry state is missing")
	}
	environment, err := hierarchyrecord.DecodeEnvironment(stored.Values[0].Value)
	if err != nil {
		return err
	}
	if environment.ID != task.Target {
		return errs.New(errs.KindStateConflict, "environment deletion retained another target")
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(stored.Values[1].Value)
	if err != nil {
		return err
	}
	if tombstone.TargetKind != deletionrecord.DeletionTargetEnvironment || tombstone.TargetID != task.Target ||
		tombstone.TargetRevision != stored.Values[0].ModRevision || tombstone.TaskID != task.ID ||
		(tombstone.Phase != deletionrecord.DeletionPhaseHostEffects && tombstone.Phase != deletionrecord.DeletionPhaseFinalizing) {
		return errs.New(
			errs.KindStateConflict,
			"environment deletion retry tombstone ownership changed",
		)
	}
	if _, err := decodeEnvironmentDeletionEpoch(stored.Values[5], environment.ID); err != nil {
		return err
	}
	if _, err := decodeOwnedEnvironmentDeletionLock(stored.Values[6], task); err != nil {
		return err
	}
	if err := repository.validateEnvironmentDeletionIntentReplay(
		ctx, task, &tombstone, terminalStatus, readRevision,
	); err != nil {
		return err
	}
	if stored.Values[4] == nil {
		return errs.New(errs.KindStateConflict, "environment deletion lost its pool")
	}
	poolRegistry, err := recordcodec.Decode[EnvironmentPoolRegistry](
		stored.Values[4].Value,
		"environment_pool_registry",
	)
	if err != nil || validateEnvironmentPoolRegistry(poolRegistry) != nil ||
		poolRegistry.Reservations[environment.ID] != environment.NetworkPool {
		return errs.New(errs.KindStateConflict, "environment deletion changed its pool")
	}
	return nil
}
