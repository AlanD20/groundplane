package etcd

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	platformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreatePlatformComponent atomically publishes the platform component and indexes.
func (repository *ComponentRepository) CreatePlatformComponent(
	ctx context.Context,
	record componentrecord.Record,
) (etcdstore.Versioned[componentrecord.Record], error) {
	return repository.createPlatformComponent(ctx, record, false)
}

func (repository *ComponentRepository) createPlatformComponent(
	ctx context.Context,
	record componentrecord.Record,
	bootstrap bool,
) (etcdstore.Versioned[componentrecord.Record], error) {
	if err := platformcomponents.ValidatePlatformComponentRecord(record); err != nil {
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
		{Key: platformcomponents.PlatformComponentOwnerKey(record.Desired.ID)},
		{
			Key: platformcomponents.PlatformComponentKindKey(record.Desired.Kind),
		},
		{Key: deletions.TombstoneKey("component", record.Desired.ID)},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: componentrecord.RecordKey(record.Desired.ID), Value: value},
		{Type: etcdstore.MutationPut, Key: platformcomponents.PlatformComponentOwnerKey(record.Desired.ID), Value: []byte(record.Desired.ID)},
		{Type: etcdstore.MutationPut, Key: platformcomponents.PlatformComponentKindKey(record.Desired.Kind), Value: []byte(record.Desired.ID)},
		componentrecord.WriteFenceMutation(record.Desired.ID),
	}
	if bootstrap {
		conditions = append(conditions, etcdstore.Condition{Key: platformComponentBootstrapKey(record.Desired.ID)})
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: platformComponentBootstrapKey(record.Desired.ID),
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
	return etcdstore.Versioned[componentrecord.Record]{Record: record, Revision: result.Revision, ReadRevision: result.Revision}, nil
}

func (repository *ComponentRepository) ListPlatformComponents(
	ctx context.Context,
	request etcdstore.PageRequest,
) (etcdstore.Page[componentrecord.Record], error) {
	return recordquery.ListIndex(ctx, repository.store, "components", "platform", "", platformcomponents.PlatformComponentOwnerPrefix,
		componentrecord.RecordKey, ids.KindComponent, request, componentrecord.DecodeRecord,
		func(record componentrecord.Record) string { return record.Desired.ID },
		func(record componentrecord.Record) bool {
			return record.Desired.Owner == core.ComponentOwnerPlatform && record.Desired.OwnerID == ""
		})
}

func (repository *ComponentRepository) ReplacePlatformDesired(
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

func (repository *ComponentRepository) ReplacePlatformRuntime(
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

func (repository *ComponentRepository) replacePlatform(
	ctx context.Context,
	current etcdstore.Versioned[componentrecord.Record],
	replacement componentrecord.Record,
) (etcdstore.Versioned[componentrecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if err := platformcomponents.ValidatePlatformComponentRecord(current.Record); err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if err := platformcomponents.ValidatePlatformComponentRecord(replacement); err != nil {
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
				platformcomponents.PlatformComponentOwnerKey(current.Record.Desired.ID),
				platformcomponents.PlatformComponentKindKey(current.Record.Desired.Kind),
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
		{Key: platformcomponents.PlatformComponentOwnerKey(current.Record.Desired.ID), ModRevision: indexes.Values[0].ModRevision},
		{Key: platformcomponents.PlatformComponentKindKey(current.Record.Desired.Kind), ModRevision: indexes.Values[1].ModRevision},
		{Key: deletions.TombstoneKey("component", current.Record.Desired.ID)},
	}, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: componentrecord.RecordKey(replacement.Desired.ID), Value: value},
		componentrecord.WriteFenceMutation(replacement.Desired.ID),
	})
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[componentrecord.Record]{}, recordcodec.StateConflict("platform component", current.Record.Desired.ID)
	}
	return etcdstore.Versioned[componentrecord.Record]{
		Record:       replacement,
		Revision:     result.Revision,
		ReadRevision: result.Revision,
	}, nil
}

// PutPlatformComponentObservation fences the component and prior observation in one transaction.
func (repository *ComponentRepository) PutPlatformComponentObservation(
	ctx context.Context,
	record platformcomponents.ComponentObservationRecord,
	expectedComponentRevision int64,
	expectedObservationRevision int64,
) (etcdstore.Versioned[platformcomponents.ComponentObservationRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{}, err
	}
	if err := platformcomponents.ValidateComponentObservation(record); err != nil {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{}, err
	}
	component, err := repository.GetComponent(ctx, record.ComponentID)
	if err != nil {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{}, err
	}
	if component.Record.Desired.Owner != core.ComponentOwnerPlatform || component.Record.Desired.OwnerID != "" ||
		component.Revision != expectedComponentRevision {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{}, recordcodec.StateConflict("platform component", record.ComponentID)
	}
	current, err := repository.store.Get(ctx, platformcomponents.ComponentObservationKey(record.ComponentID))
	if err != nil {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{}, err
	}
	if current == nil || revisionChanged(current.Entry, expectedObservationRevision) {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{}, recordcodec.StateConflict(
			"platform component observation",
			record.ComponentID,
		)
	}
	value, err := platformcomponents.EncodeComponentObservation(record)
	if err != nil {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{}, err
	}
	defer clear(value)
	result, err := repository.store.Transact(
		ctx,
		[]etcdstore.Condition{
			{Key: componentrecord.RecordKey(record.ComponentID), ModRevision: expectedComponentRevision},
			{Key: platformcomponents.ComponentObservationKey(record.ComponentID), ModRevision: expectedObservationRevision},
			{Key: deletions.TombstoneKey("component", record.ComponentID)},
		},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: platformcomponents.ComponentObservationKey(record.ComponentID), Value: value}},
	)
	if err != nil {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{}, recordcodec.StateConflict(
			"platform component observation",
			record.ComponentID,
		)
	}
	return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{
		Record:       record,
		Revision:     result.Revision,
		ReadRevision: result.Revision,
	}, nil
}

func (repository *ComponentRepository) GetPlatformComponentObservation(
	ctx context.Context,
	componentID string,
) (etcdstore.Versioned[platformcomponents.ComponentObservationRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{}, false, err
	}
	if err := ids.Validate(ids.KindComponent, componentID); err != nil {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{}, false, errs.New(errs.KindValidationFailed, err.Error())
	}
	result, err := repository.store.Get(ctx, platformcomponents.ComponentObservationKey(componentID))
	if err != nil {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{}, false, errs.New(
			errs.KindInternal,
			"Component observation read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := platformcomponents.DecodeComponentObservation(result.Entry.Value)
	if err != nil || record.ComponentID != componentID {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{}, false, errs.New(
			errs.KindInternal,
			"Component observation is corrupt",
		)
	}
	return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{
		Record:       record,
		Revision:     result.Entry.ModRevision,
		ReadRevision: result.ReadRevision,
	}, true, nil
}
