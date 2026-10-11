package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintretention"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Retire one bounded batch before allowing the expired parent journal to go.
// Every batch rechecks its owners. The immutable root remains until the final
// batch, so interruption cannot leave a live revision with missing authority.
func (repository *TaskRepository) pruneParentOwnedBlueprintRevision(
	ctx context.Context, task TaskRecord, taskRevision int64,
	retention keyvalue.KeyValue, revision int64,
) (bool, error) {
	if !taskOwnsBlueprintRevision(task) {
		if task.Executor == taskjournal.TaskExecutorBlueprint {
			return true, errs.New(errs.KindInternal, "Blueprint revision prune owner is invalid")
		}
		return false, nil
	}
	if task.Params[blueprints.EnvironmentDesiredRevisionParam] != task.ID ||
		ids.Validate(ids.KindEnvironment, task.Owner.EnvironmentID) != nil ||
		!taskjournal.IsTerminalTaskStatus(task.Status) {
		return true, errs.New(errs.KindInternal, "Blueprint revision prune owner is invalid")
	}
	conditions, held, err := blueprintretention.Guards(
		ctx,
		repository.store,
		task.Owner.EnvironmentID,
		task.ID,
		revision,
	)
	if err != nil || held {
		return true, err
	}
	prefix := blueprints.EnvironmentBlueprintRevisionPrefixFinal(task.Owner.EnvironmentID, task.ID)
	page, err := repository.store.Range(ctx, keyvalue.RangeRequest{Prefix: prefix, Limit: 33, Revision: revision})
	if err != nil {
		return true, err
	}
	if page == nil || page.ReadRevision != revision || len(page.Values) > 33 {
		return true, errs.New(errs.KindInternal, "Blueprint revision prune page is incomplete")
	}
	defer keyvalue.ClearRangeValues(page.Values)
	if len(page.Values) == 0 {
		return false, nil
	}
	rootKey := blueprints.EnvironmentBlueprintRootKey(task.Owner.EnvironmentID, task.ID)
	rootRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{rootKey}, Revision: revision})
	if err != nil {
		return true, err
	}
	if rootRead == nil || rootRead.ReadRevision != revision || len(rootRead.Values) != 1 || rootRead.Values[0] == nil {
		return true, errs.New(errs.KindStateConflict, "Blueprint revision prune has no immutable root owner")
	}
	defer keyvalue.ClearValues(rootRead.Values)
	root, err := blueprints.DecodeEnvironmentBlueprintSeal(rootRead.Values[0].Value)
	if err != nil || root.EnvironmentID != task.Owner.EnvironmentID || root.RevisionID != task.ID ||
		root.RenderGeneration != uint64(task.RenderGeneration) {
		return true, errs.New(errs.KindInternal, "Blueprint revision prune root ownership is corrupt")
	}
	conditions = append(conditions,
		keyvalue.Condition{Key: rootKey, ModRevision: rootRead.Values[0].ModRevision},
		keyvalue.Condition{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRevision},
		keyvalue.Condition{Key: retention.Key, ModRevision: retention.ModRevision},
		keyvalue.Condition{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
	)
	var mutations []keyvalue.Mutation
	for _, value := range page.Values {
		if value.Key == rootKey && (len(page.Values) != 1 || page.More) {
			continue
		}
		if value.Key != rootKey {
			conditions = append(conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
		}
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: value.Key})
		if len(mutations) == 32 {
			break
		}
	}
	if len(mutations) == 0 {
		return true, errs.New(errs.KindInternal, "Blueprint revision prune made no progress")
	}
	if err := ValidateBlueprintTransaction(repository.store, conditions, mutations, 128,
		blueprints.EnvironmentBlueprintGCTransactionBytes); err != nil {
		return true, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return true, err
	}
	keyvalue.ClearValues(result.FailureReads)
	if !result.Succeeded {
		return true, errs.New(errs.KindStateConflict, "Blueprint revision prune ownership changed")
	}
	return true, nil
}

func taskOwnsBlueprintRevision(task TaskRecord) bool {
	return task.Params[blueprints.EnvironmentDesiredRevisionParam] == task.ID &&
		ids.Validate(ids.KindEnvironment, task.Owner.EnvironmentID) == nil
}
