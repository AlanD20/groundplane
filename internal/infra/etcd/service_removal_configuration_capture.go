package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Capture file authority at the removal planning revision. The last deployment's
// snapshot is not a substitute: Entry edits may have changed files since then.
func (repository *ServiceRepository) captureRemovalConfiguration(ctx context.Context, input serviceremovalwork.Input,
	revision int64,
) (serviceremovalwork.Input, []keyvalue.Condition, error) {
	if input.Runtime == nil {
		return input, nil, input.Validate()
	}
	if revision <= 0 || ids.Validate(ids.KindEnvironment, input.Scope.EnvironmentID) != nil ||
		serviceruntimerecord.Validate(
			*input.Runtime,
		) != nil || input.Runtime.EnvironmentID != input.Scope.EnvironmentID ||
		input.Configuration != nil || input.ConfigurationRevision != 0 {
		return input, nil, errs.New(
			errs.KindValidationFailed,
			"Service removal configuration capture requires an uncaptured runtime input",
		)
	}
	key := runtimeconfiguration.HeadKey(input.Scope.EnvironmentID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return input, nil, err
	}
	if read == nil {
		return input, nil, errs.New(errs.KindStateConflict, "Service removal configuration read is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != revision || len(read.Values) != 1 {
		return input, nil, errs.New(errs.KindStateConflict, "Service removal configuration read is inconsistent")
	}
	guard := keyvalue.Condition{Key: key}
	if value := read.Values[0]; value != nil {
		if value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision {
			return input, nil, errs.New(errs.KindStateConflict, "Service removal configuration revision is invalid")
		}
		reference, err := runtimeconfiguration.DecodeReference(value.Value)
		if err != nil {
			return input, nil, err
		}
		input.Configuration, input.ConfigurationRevision = &reference, value.ModRevision
		guard.ModRevision = value.ModRevision
		sources, err := runtimeconfiguration.New(repository.store)
		if err != nil {
			return input, nil, err
		}
		snapshot, err := sources.Load(ctx, reference, revision)
		if err != nil {
			return input, nil, err
		}
		if _, err := serviceruntimerecord.ConfigurationFiles(ctx, *input.Runtime, snapshot); err != nil {
			return input, nil, err
		}
	}
	return input, []keyvalue.Condition{guard}, input.Validate()
}
