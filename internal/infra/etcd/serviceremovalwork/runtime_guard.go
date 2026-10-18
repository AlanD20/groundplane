package serviceremovalwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

// RuntimeGuards fences the resource side of a removal boundary. Callers must
// also compare native Task/item ownership and the selected Blueprint unit before
// authorizing effects. A tombstone alone is not permission to execute.
func (repository *Repository) RuntimeGuards(ctx context.Context, input Input, revision int64) ([]keyvalue.Condition, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	if revision <= 0 {
		return nil, conflict("Service removal requires a consistent resource snapshot")
	}
	keys := []string{
		deletions.TombstoneKey(string(deletions.DeletionTargetService), input.ServiceID),
		services.ServiceRuntimeKey(input.ServiceID),
		serviceruntimerecord.Key(input.ServiceID),
	}
	if input.Runtime != nil {
		keys = append(keys, runtimeconfiguration.HeadKey(input.Scope.EnvironmentID))
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil {
		return nil, conflict("Service removal resource snapshot is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, conflict("Service removal resource snapshot is inconsistent")
	}
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		conditions[index].Key = key
		if stored := read.Values[index]; stored != nil {
			if stored.Key != key || stored.ModRevision <= 0 || stored.ModRevision > revision {
				return nil, conflict("Service removal resource revision is invalid")
			}
			conditions[index].ModRevision = stored.ModRevision
		}
	}
	if read.Values[0] == nil {
		return nil, conflict("Service removal no longer owns its deletion fence")
	}
	if err := validateDeletionFence(input, read.Values[0].Value); err != nil {
		return nil, err
	}
	if conditions[1].ModRevision != input.RuntimeRevision || conditions[2].ModRevision != input.RuntimeSourceRevision {
		return nil, conflict("Service runtime changed after removal was requested")
	}
	if input.Runtime != nil {
		if conditions[3].ModRevision != input.ConfigurationRevision {
			return nil, conflict("Environment files changed after Service removal was captured")
		}
		if stored := read.Values[3]; stored != nil {
			reference, err := runtimeconfiguration.DecodeReference(stored.Value)
			if err != nil || input.Configuration == nil || reference != *input.Configuration {
				return nil, conflict("Service removal configuration no longer matches its captured source")
			}
		}
	}
	if stored := read.Values[1]; stored != nil {
		runtime, err := services.DecodeServiceRuntimeRecord(stored.Value)
		if err != nil {
			return nil, err
		}
		if runtime.ServiceID != input.ServiceID || runtime.EnvironmentID != input.Scope.EnvironmentID {
			return nil, conflict("Service removal runtime belongs to another resource")
		}
	}
	pins, err := repository.configurationPinGuard(ctx, input, revision)
	if err != nil {
		return nil, err
	}
	return append(conditions, pins...), nil
}
