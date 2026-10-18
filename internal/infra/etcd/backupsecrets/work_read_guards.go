package backupsecrets

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// credentialReadGuards includes absent deletion fences and lookup candidates,
// not only the selected encrypted value. It retains revisions, never ciphertext.
type credentialReadGuards struct {
	store      backupSecretResolutionStore
	revision   int64
	conditions map[string]keyvalue.Condition
}

func (tracked *credentialReadGuards) GetMany(ctx context.Context, request keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error) {
	if request.Revision != tracked.revision || tracked.revision <= 0 {
		return nil, errs.New(errs.KindStateConflict, "Backup credential reads must use one captured revision")
	}
	read, err := tracked.store.GetMany(ctx, request)
	if err != nil {
		return nil, err
	}
	if read == nil {
		return nil, errs.New(errs.KindStateConflict, "Backup credential read is unavailable")
	}
	invalid := func() (*keyvalue.GetManyResult, error) {
		keyvalue.ClearValues(read.Values)
		return nil, errs.New(errs.KindStateConflict, "Backup credential read changed its captured snapshot")
	}
	if read.ReadRevision != tracked.revision || len(read.Values) != len(request.Keys) {
		return invalid()
	}
	for index, key := range request.Keys {
		condition := keyvalue.Condition{Key: key}
		if value := read.Values[index]; value != nil {
			if value.Key != key || value.ModRevision <= 0 || value.ModRevision > tracked.revision {
				return invalid()
			}
			condition.ModRevision = value.ModRevision
		}
		if prior, found := tracked.conditions[key]; found && prior.ModRevision != condition.ModRevision {
			return invalid()
		}
		tracked.conditions[key] = condition
	}
	return read, nil
}

func (tracked *credentialReadGuards) sortedConditions() []keyvalue.Condition {
	keys := make([]string, 0, len(tracked.conditions))
	for key := range tracked.conditions {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	result := make([]keyvalue.Condition, 0, len(keys))
	for _, key := range keys {
		result = append(result, tracked.conditions[key])
	}
	return result
}
