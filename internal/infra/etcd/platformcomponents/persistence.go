package platformcomponents

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreatePlatformComponent atomically publishes the platform component and indexes.
func (repository *Persistence) CreatePlatformComponent(
	ctx context.Context,
	record componentrecord.Record,
) (etcdstore.Versioned[componentrecord.Record], error) {
	return repository.createPlatformComponent(ctx, record, false)
}

func (repository *Persistence) createPlatformComponent(
	ctx context.Context,
	record componentrecord.Record,
	bootstrap bool,
) (etcdstore.Versioned[componentrecord.Record], error) {
	if err := ValidatePlatformComponentRecord(record); err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if bootstrap && (record.Desired.ID != platformCoreDNSComponentID ||
		record.Desired.Kind != core.ComponentKindCoreDNS || !record.Desired.Enabled ||
		len(record.Runtime.GeneratedServices) != 0 || record.Runtime.PinnedIPv4 != "" || record.Runtime.Healthy) {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"platform Component bootstrap record is invalid",
		)
	}
	value, err := componentrecord.EncodeRecord(record)
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	defer clear(value)
	conditions := []etcdstore.Condition{
		{Key: componentrecord.RecordKey(record.Desired.ID)},
		{Key: PlatformComponentOwnerKey(record.Desired.ID)},
		{
			Key: PlatformComponentKindKey(record.Desired.Kind),
		},
		{Key: deletions.TombstoneKey("component", record.Desired.ID)},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: componentrecord.RecordKey(record.Desired.ID), Value: value},
		{
			Type:  etcdstore.MutationPut,
			Key:   PlatformComponentOwnerKey(record.Desired.ID),
			Value: []byte(record.Desired.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   PlatformComponentKindKey(record.Desired.Kind),
			Value: []byte(record.Desired.ID),
		},
		componentrecord.WriteFenceMutation(record.Desired.ID),
	}
	if bootstrap {
		conditions = append(conditions, etcdstore.Condition{Key: PlatformComponentBootstrapKey(record.Desired.ID)})
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: PlatformComponentBootstrapKey(record.Desired.ID),
			Value: []byte(record.Desired.ID),
		})
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
			errs.KindStateConflict,
			"platform Component stable identity is already in use",
		)
	}
	return etcdstore.Versioned[componentrecord.Record]{
		Record:       record,
		Revision:     result.Revision,
		ReadRevision: result.Revision,
	}, nil
}

func (repository *Persistence) ListPlatformComponents(
	ctx context.Context,
	request etcdstore.PageRequest,
) (etcdstore.Page[componentrecord.Record], error) {
	return recordquery.ListIndex(ctx, repository.store, "components", "platform", "", PlatformComponentOwnerPrefix,
		componentrecord.RecordKey, ids.KindComponent, request, componentrecord.DecodeRecord,
		func(record componentrecord.Record) string { return record.Desired.ID },
		func(record componentrecord.Record) bool {
			return record.Desired.Owner == core.ComponentOwnerPlatform && record.Desired.OwnerID == ""
		})
}

func (repository *Persistence) ReplacePlatformDesired(
	ctx context.Context,
	current etcdstore.Versioned[componentrecord.Record],
	desired core.Component,
) (etcdstore.Versioned[componentrecord.Record], error) {
	replacement, err := componentrecord.ReplaceDesired(current.Record, desired)
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	return repository.replacePlatform(ctx, current, replacement)
}

func (repository *Persistence) ReplacePlatformRuntime(
	ctx context.Context,
	current etcdstore.Versioned[componentrecord.Record],
	generatedServices []string,
	pinnedIPv4 string,
	healthy bool,
) (etcdstore.Versioned[componentrecord.Record], error) {
	replacement, err := componentrecord.SetRuntime(current.Record, generatedServices, pinnedIPv4, healthy)
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	return repository.replacePlatform(ctx, current, replacement)
}

func (repository *Persistence) replacePlatform(
	ctx context.Context,
	current etcdstore.Versioned[componentrecord.Record],
	replacement componentrecord.Record,
) (etcdstore.Versioned[componentrecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if err := ValidatePlatformComponentRecord(current.Record); err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if err := ValidatePlatformComponentRecord(replacement); err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision ||
		current.Record.Desired.ID != replacement.Desired.ID {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"platform Component version metadata is invalid",
		)
	}
	indexes, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{
			Keys: []string{
				PlatformComponentOwnerKey(current.Record.Desired.ID),
				PlatformComponentKindKey(current.Record.Desired.Kind),
			},
			Revision: current.ReadRevision,
		},
	)
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
			errs.KindInternal,
			"platform Component indexes are missing or corrupt",
		)
	}
	value, err := componentrecord.EncodeRecord(replacement)
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	defer clear(value)
	result, err := repository.store.Transact(ctx, []etcdstore.Condition{
		{Key: componentrecord.RecordKey(current.Record.Desired.ID), ModRevision: current.Revision},
		{Key: PlatformComponentOwnerKey(current.Record.Desired.ID), ModRevision: indexes.Values[0].ModRevision},
		{Key: PlatformComponentKindKey(current.Record.Desired.Kind), ModRevision: indexes.Values[1].ModRevision},
		{Key: deletions.TombstoneKey("component", current.Record.Desired.ID)},
	}, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: componentrecord.RecordKey(replacement.Desired.ID), Value: value},
		componentrecord.WriteFenceMutation(replacement.Desired.ID),
	})
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[componentrecord.Record]{}, recordcodec.StateConflict(
			"platform component",
			current.Record.Desired.ID,
		)
	}
	return etcdstore.Versioned[componentrecord.Record]{
		Record:       replacement,
		Revision:     result.Revision,
		ReadRevision: result.Revision,
	}, nil
}
