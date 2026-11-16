package componentwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RejectMissingCredential closes never-executable work when its accepted stable
// credential identity has disappeared. A new credential with the same key is a
// different resource and cannot repair that immutable input. Storage failures
// and resources that can legitimately become ready later remain retryable.
func (repository *InputRepository) RejectMissingCredential(
	ctx context.Context,
	view workqueue.View,
	headRevision int64,
) (bool, error) {
	task, item := view.Task.Task, view.Item.Item
	if headRevision <= 0 || headRevision > view.ReadRevision {
		return false, errs.New(
			errs.KindStateConflict,
			"Component credential check requires its accepted Blueprint head",
		)
	}
	desired, found, err := blueprints.ReadDesiredInputRevisionAt(ctx, repository.store,
		task.Scope.EnvironmentID, task.ID, view.ReadRevision)
	if err != nil {
		return false, err
	}
	if !found {
		return false, errs.New(errs.KindStateConflict, "Accepted Component configuration is unavailable")
	}
	owned, found, err := blueprints.ReadOwnedIdentitiesRevision(ctx, repository.store,
		task.Scope.EnvironmentID, task.ID, view.ReadRevision)
	if err != nil {
		return false, err
	}
	if !found {
		return false, errs.New(errs.KindStateConflict, "Accepted Component identity is unavailable")
	}
	secretID := ""
	for _, identity := range owned.Record.Components {
		if identity.ID == item.TargetID {
			secretID = desired.Record.Input.Components[identity.Name].Settings.SecretID
			break
		}
	}
	if secretID == "" {
		return false, nil
	}
	key := secrets.RecordKey(secretID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision},
	)
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 {
		return false, errs.New(errs.KindInternal, "Component credential read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] != nil {
		return false, nil
	}
	change, err := workqueue.PrepareInputRejection(
		view,
		"The credential selected when this Task was accepted no longer exists. Select an existing credential and apply the Component again; recreating its key does not change this Task.",
		time.Now().UTC(),
	)
	if err != nil {
		return false, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: key},
		keyvalue.Condition{
			Key:         blueprints.EnvironmentBlueprintHeadKey(task.Scope.EnvironmentID),
			ModRevision: headRevision,
		},
		keyvalue.Condition{
			Key:         blueprints.EnvironmentBlueprintRootKey(task.Scope.EnvironmentID, task.ID),
			ModRevision: desired.Revision,
		},
		keyvalue.Condition{
			Key:         blueprints.EnvironmentBlueprintOwnedIdentitiesKey(task.Scope.EnvironmentID, task.ID),
			ModRevision: owned.Revision,
		},
	)
	result, err := repository.transactions.Commit(ctx, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return false, err
	}
	if !result.Succeeded {
		return false, errs.New(errs.KindStateConflict, "Component credential or preparation changed before rejection")
	}
	return true, nil
}
