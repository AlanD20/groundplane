package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func environmentDeletionIntentMatches(
	record deletionrecord.EnvironmentDeletionIntentRecord,
	task TaskRecord,
	tombstone deletionrecord.DeletionTombstoneRecord,
) bool {
	return record.EnvironmentID == task.Target && record.OperationID == task.OperationID &&
		record.TaskID == task.ID && record.TargetRevision == tombstone.TargetRevision &&
		record.CreatedAt.Equal(tombstone.CreatedAt)
}

func loadEnvironmentDeletionIntent(
	ctx context.Context,
	store hierarchyStore,
	task TaskRecord,
	tombstone deletionrecord.DeletionTombstoneRecord,
	revision int64,
) (etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord], error) {
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{deletionrecord.EnvironmentDeletionIntentKey(task.OperationID)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, err
	}
	if result == nil {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindInternal,
			"environment deletion intent evidence is incomplete",
		)
	}
	if result.ReadRevision != revision || len(result.Values) != 1 {
		etcdstore.ClearValues(result.Values)
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindInternal,
			"environment deletion intent evidence is incomplete",
		)
	}
	defer etcdstore.ClearValues(result.Values)
	if result.Values[0] == nil {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion intent is missing",
		)
	}
	record, err := deletionrecord.DecodeEnvironmentDeletionIntent(result.Values[0].Value)
	if err != nil {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, err
	}
	if !environmentDeletionIntentMatches(record, task, tombstone) {
		return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion intent ownership changed",
		)
	}
	return etcdstore.Versioned[deletionrecord.EnvironmentDeletionIntentRecord]{
		Record: record, Revision: result.Values[0].ModRevision, ReadRevision: revision,
	}, nil
}

func classifyEnvironmentDeletionIntentStartConflict(
	base idempotencyPlanClassifier,
) idempotencyPlanClassifier {
	return func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) == 0 {
			return errs.New(
				errs.KindInternal,
				"environment deletion compare evidence is incomplete",
			)
		}
		intent := values[len(values)-1]
		if intent != nil {
			return errs.New(errs.KindInternal, "environment deletion intent already exists")
		}
		return base(revision, values[:len(values)-1])
	}
}
