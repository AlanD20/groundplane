// Package postgresbackingguard owns PostgreSQL backing Environment lock
// acquisition, ownership proof and release transaction fragments.
package postgresbackingguard

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type store interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

// Plan is an owned fragment for the caller's enclosing atomic transaction.
type Plan struct {
	Conditions []etcdstore.Condition
	Mutations  []etcdstore.Mutation
}

// Clear releases mutation values owned by the plan.
func (plan *Plan) Clear() {
	if plan == nil {
		return
	}
	etcdstore.ClearMutationValues(plan.Mutations)
	plan.Conditions = nil
	plan.Mutations = nil
}

// PrepareAcquisition validates the complete ordinary Environment fence at the
// fixed planning revision, then uses its canonical mutation epoch as the
// compact atomic acquisition compare and rewrites that epoch with the lock put.
// Concurrent acquisitions therefore cannot overwrite each other, and every
// planner that predates the lock remains invalid after its exact CAS deletion.
func PrepareAcquisition(
	ctx context.Context,
	storage store,
	environmentIDs []string,
	consumerEnvironmentID string,
	owner environmentfence.Owner,
	createdAt time.Time,
	readRevision int64,
) (Plan, error) {
	plan := Plan{
		Conditions: make([]etcdstore.Condition, 0, len(environmentIDs)),
		Mutations:  make([]etcdstore.Mutation, 0, len(environmentIDs)*2),
	}
	for _, environmentID := range environmentIDs {
		if environmentID == consumerEnvironmentID {
			continue
		}
		fence, err := environmentfence.LoadOrdinary(ctx, storage, environmentID, readRevision)
		if err != nil {
			plan.Clear()
			return Plan{}, err
		}
		epochRevision, found := fence.EpochRevision()
		if !found || epochRevision <= 0 {
			plan.Clear()
			return Plan{}, errs.New(
				errs.KindInternal,
				"PostgreSQL backing Environment mutation epoch is missing",
			)
		}
		lockValue, err := backupruntime.EncodeBackupOperationLockRecord(
			backupruntime.BackupOperationLockRecord{
				EnvironmentID: environmentID,
				OperationID:   owner.OperationID,
				TaskID:        owner.TaskID,
				Kind:          owner.Kind,
				CreatedAt:     createdAt,
				UpdatedAt:     createdAt,
			},
		)
		if err != nil {
			plan.Clear()
			return Plan{}, err
		}
		epochMutation, err := fence.EpochRewriteMutation()
		if err != nil {
			clear(lockValue)
			plan.Clear()
			return Plan{}, err
		}
		plan.Conditions = append(plan.Conditions, etcdstore.Condition{
			Key:         hierarchy.EnvironmentMutationEpochKey(environmentID),
			ModRevision: epochRevision,
		})
		plan.Mutations = append(
			plan.Mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   hierarchy.EnvironmentOperationLockKey(environmentID),
				Value: lockValue,
			},
			epochMutation,
		)
	}
	return plan, nil
}

// PrepareOwnership proves that every selected backing Environment still
// carries the original operation lock. A safe terminal may atomically delete
// those locks; uncertain terminals retain them unchanged.
func PrepareOwnership(
	ctx context.Context,
	storage store,
	environmentIDs []string,
	consumerEnvironmentID string,
	owner environmentfence.Owner,
	readRevision int64,
	release bool,
) (Plan, error) {
	plan := Plan{Conditions: make([]etcdstore.Condition, 0, len(environmentIDs))}
	if release {
		plan.Mutations = make([]etcdstore.Mutation, 0, len(environmentIDs))
	}
	for _, environmentID := range environmentIDs {
		if environmentID == consumerEnvironmentID {
			continue
		}
		fence, err := environmentfence.LoadOwned(ctx, storage, environmentID, readRevision, owner)
		if err != nil {
			plan.Clear()
			return Plan{}, err
		}
		lockCondition, found := lockCondition(fence, environmentID)
		if !found {
			plan.Clear()
			return Plan{}, errs.New(
				errs.KindInternal,
				"PostgreSQL backing Environment lock compare is missing",
			)
		}
		plan.Conditions = append(plan.Conditions, lockCondition)
		if release {
			plan.Mutations = append(plan.Mutations, etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  hierarchy.EnvironmentOperationLockKey(environmentID),
			})
		}
	}
	return plan, nil
}

func lockCondition(fence environmentfence.Evidence, environmentID string) (etcdstore.Condition, bool) {
	key := hierarchy.EnvironmentOperationLockKey(environmentID)
	for _, condition := range fence.TransactionConditions() {
		if condition.Key == key && condition.ModRevision > 0 && !condition.Prefix {
			return condition, true
		}
	}
	return etcdstore.Condition{}, false
}

// Owner constructs the exact Environment-fence owner for a Backup operation.
func Owner(kind backupruntime.BackupOperationKind, operationID string, taskID string) environmentfence.Owner {
	return environmentfence.Owner{Kind: kind, OperationID: operationID, TaskID: taskID}
}
