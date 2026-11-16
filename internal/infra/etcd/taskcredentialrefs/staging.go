package taskcredentialrefs

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Stage protects references in bounded batches before acceptance. Its final
// activation fragment joins the original Task and replay publication atomically.
// Partial submissions have an expiry and remain safe for bounded collection.
func (repository *Repository) Stage(
	ctx context.Context,
	task workexecution.Task,
	secretIDs []string,
) (publication Publication, err error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return publication, err
	}
	if len(secretIDs) == 0 {
		return publication, nil
	}
	if task.Validate() != nil || !task.PendingPreparation() || len(secretIDs) > maximumCredentials {
		return publication, errs.New(errs.KindValidationFailed, "credential references require a pure accepted Task")
	}
	secretIDs = slices.Clone(secretIDs)
	slices.Sort(secretIDs)
	secretIDs = slices.Compact(secretIDs)
	value := record{TaskID: task.ID, ProjectID: task.Scope.ProjectID, Intent: *task.AcceptedIntent,
		SecretIDs: secretIDs, ExpiresAt: time.Now().UTC().Add(time.Hour)}
	raw, err := encode(value)
	if err != nil {
		return publication, err
	}
	defer clear(raw)
	result, err := repository.store.Transact(ctx,
		[]keyvalue.Condition{{Key: Key(task.ID)}, {Key: taskKey(task.ID)}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: Key(task.ID), Value: raw}})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return publication, err
	}
	if !result.Succeeded {
		return publication, errs.New(errs.KindStateConflict, "pending credential reference owner changed")
	}
	defer func() {
		if err == nil {
			return
		}
		bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if cleanup := repository.Finish(bounded, task.ID, false); cleanup != nil {
			err = errs.WrapJoined(errs.KindInternal, err, cleanup)
		}
	}()
	revision := result.Revision
	for value.Published < len(secretIDs) {
		end := min(value.Published+8, len(secretIDs))
		keys := make([]string, 0, (end-value.Published)*2)
		for _, id := range secretIDs[value.Published:end] {
			keys = append(
				keys,
				secrets.RecordKey(id),
				deletions.TombstoneKey(string(deletions.DeletionTargetSecret), id),
			)
		}
		read, readErr := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
		if readErr != nil {
			return publication, readErr
		}
		if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
			return publication, corrupt()
		}
		conditions := []keyvalue.Condition{{Key: Key(task.ID), ModRevision: revision}, {Key: taskKey(task.ID)}}
		var mutations []keyvalue.Mutation
		for index, id := range secretIDs[value.Published:end] {
			entry, tombstone := read.Values[index*2], read.Values[index*2+1]
			if entry == nil {
				keyvalue.ClearValues(read.Values)
				return publication, errs.New(
					errs.KindSecretNotFound,
					"A credential selected by this request no longer exists",
				)
			}
			credential, decodeErr := secrets.DecodeRecord(entry.Value)
			if decodeErr != nil || credential.Secret.ID != id || entry.Key != keys[index*2] ||
				string(credential.Secret.Scope) == "project" && credential.Secret.ProjectID != value.ProjectID ||
				string(credential.Secret.Scope) != "project" && string(credential.Secret.Scope) != "platform" {
				keyvalue.ClearValues(read.Values)
				return publication, errs.New(
					errs.KindValidationFailed,
					"A selected credential is outside this action's Project",
				)
			}
			if tombstone != nil {
				keyvalue.ClearValues(read.Values)
				return publication, errs.New(errs.KindResourceInUse, "A selected credential is already being removed")
			}
			key := SecretPrefix(id) + task.ID
			conditions = append(conditions, keyvalue.Condition{Key: entry.Key, ModRevision: entry.ModRevision},
				keyvalue.Condition{Key: keys[index*2+1]}, keyvalue.Condition{Key: key})
			mutations = append(
				mutations,
				keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: []byte(task.ID)},
			)
		}
		keyvalue.ClearValues(read.Values)
		value.Published = end
		encoded, encodeErr := encode(value)
		if encodeErr != nil {
			return publication, encodeErr
		}
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: Key(task.ID), Value: encoded})
		result, err = repository.store.Transact(ctx, conditions, mutations)
		keyvalue.ClearMutationValues(mutations)
		keyvalue.ClearValues(result.FailureReads)
		if err != nil {
			return publication, err
		}
		if !result.Succeeded {
			return publication, errs.New(
				errs.KindStateConflict,
				"A credential changed before this request was accepted",
			)
		}
		revision = result.Revision
	}
	value.Accepted = true
	sealed, err := encode(value)
	if err != nil {
		return publication, err
	}
	return Publication{Conditions: []keyvalue.Condition{{Key: Key(task.ID), ModRevision: revision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: Key(task.ID), Value: sealed}}}, nil
}
