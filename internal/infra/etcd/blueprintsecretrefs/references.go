// Package blueprintsecretrefs protects reusable credentials named by accepted
// Component configuration before runtime metadata or recovery pins exist.
package blueprintsecretrefs

import (
	"context"
	"slices"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

func EnvironmentKey(environmentID string) string {
	return "/v1/indexes/blueprint-secrets/by-environment/" + environmentID
}

func SecretPrefix(secretID string) string {
	return "/v1/indexes/blueprint-secrets/by-secret/" + secretID + "/"
}

type references struct {
	EnvironmentID string   `json:"environment_id"`
	SecretIDs     []string `json:"secret_ids"`
}

// PrepareReplacement must be joined to the desired-head CAS. Each reverse
// reference and the Secret removal tombstone are compared in that same commit,
// so either acceptance protects the credential or removal wins first.
func PrepareReplacement(ctx context.Context, store Store, environmentID, projectID string,
	nextIDs []string, revision int64,
) (workqueue.Change, error) {
	if keyvalue.ValidateContext(ctx) != nil || store == nil || revision <= 0 ||
		recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil ||
		recordcodec.ValidateID(ids.KindProject, projectID) != nil || len(nextIDs) > 2 {
		return workqueue.Change{}, invalid("Blueprint credential reference identity is invalid")
	}
	nextIDs = slices.Clone(nextIDs)
	sort.Strings(nextIDs)
	nextIDs = slices.Compact(nextIDs)
	for _, id := range nextIDs {
		if recordcodec.ValidateID(ids.KindSecret, id) != nil {
			return workqueue.Change{}, invalid("Blueprint credential reference is invalid")
		}
	}
	key := EnvironmentKey(environmentID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return workqueue.Change{}, corrupt()
	}
	defer keyvalue.ClearValues(read.Values)
	var previous references
	if value := read.Values[0]; value != nil {
		previous, err = recordcodec.Decode[references](value.Value, "blueprint-secret-references")
		if err != nil || value.Key != key || previous.EnvironmentID != environmentID ||
			len(previous.SecretIDs) == 0 || len(previous.SecretIDs) > 2 || !slices.IsSorted(previous.SecretIDs) {
			return workqueue.Change{}, corrupt()
		}
		for index, id := range previous.SecretIDs {
			if recordcodec.ValidateID(ids.KindSecret, id) != nil || index > 0 && previous.SecretIDs[index-1] == id {
				return workqueue.Change{}, corrupt()
			}
		}
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])}}}
	if len(previous.SecretIDs) == 0 && len(nextIDs) == 0 {
		return workqueue.Change{}, nil
	}
	keys := make([]string, 0, len(nextIDs)*2)
	for _, id := range nextIDs {
		keys = append(keys, secrets.RecordKey(id), deletions.TombstoneKey(string(deletions.DeletionTargetSecret), id))
	}
	if len(keys) != 0 {
		credentials, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
		if err != nil {
			return workqueue.Change{}, err
		}
		if credentials == nil || credentials.ReadRevision != revision || len(credentials.Values) != len(keys) {
			return workqueue.Change{}, corrupt()
		}
		defer keyvalue.ClearValues(credentials.Values)
		for index, id := range nextIDs {
			value, tombstone := credentials.Values[index*2], credentials.Values[index*2+1]
			if value == nil {
				return workqueue.Change{}, errs.New(errs.KindSecretNotFound, "The Component credential no longer exists; select an existing credential before applying")
			}
			record, err := secrets.DecodeRecord(value.Value)
			if err != nil || value.Key != keys[index*2] || record.Secret.ID != id ||
				string(record.Secret.Kind) != "env_var" ||
				string(record.Secret.Scope) == "project" && record.Secret.ProjectID != projectID ||
				string(record.Secret.Scope) != "project" && string(record.Secret.Scope) != "platform" {
				return workqueue.Change{}, invalid("Component credential is unavailable in this Project")
			}
			if tombstone != nil {
				return workqueue.Change{}, errs.New(errs.KindResourceInUse, "Component credential removal has already started")
			}
			change.Conditions = append(change.Conditions,
				keyvalue.Condition{Key: keys[index*2], ModRevision: value.ModRevision},
				keyvalue.Condition{Key: keys[index*2+1]})
		}
	}
	if slices.Equal(previous.SecretIDs, nextIDs) {
		return change, nil
	}
	for _, id := range previous.SecretIDs {
		if !slices.Contains(nextIDs, id) {
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: SecretPrefix(id) + environmentID})
		}
	}
	for _, id := range nextIDs {
		if !slices.Contains(previous.SecretIDs, id) {
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
				Key: SecretPrefix(id) + environmentID, Value: []byte(environmentID)})
		}
	}
	if len(nextIDs) == 0 {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
		return change, nil
	}
	value, err := recordcodec.Encode("blueprint-secret-references", references{EnvironmentID: environmentID, SecretIDs: nextIDs})
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return change, nil
}

func invalid(detail string) error { return errs.New(errs.KindValidationFailed, detail) }
func corrupt() error {
	return errs.New(errs.KindInternal, "Blueprint credential references are corrupt")
}
