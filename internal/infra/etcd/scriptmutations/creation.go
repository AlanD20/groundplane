package scriptmutations

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) CreateScript(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	record scriptrecord.Record,
) (etcdstore.Versioned[scriptrecord.Record], error) {
	conditions, mutations, classify, err := repository.PrepareScriptCreation(ctx, environment, project, target, record)
	if err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	defer etcdstore.ClearMutationValues(mutations)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[scriptrecord.Record]{}, classify(result.Revision, result.FailureReads)
	}
	created, err := scriptrecord.ReadActiveScriptStorage(ctx, repository.store, record.Desired.ID, result.Revision)
	if err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	return repository.HydrateScriptBody(ctx, created.Script)
}

func (repository *Repository) PrepareScriptCreation(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	record scriptrecord.Record,
) ([]etcdstore.Condition, []etcdstore.Mutation, func(int64, []*etcdstore.KeyValue) error, error) {
	if err := ValidateScriptHierarchy(ctx, environment, project, target, record); err != nil {
		return nil, nil, nil, err
	}
	active, err := scriptrecord.ReadActiveScriptSet(
		ctx,
		repository.store,
		record.EnvironmentID,
		environment.ReadRevision,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	record.ScriptSetGeneration = active.Record.GenerationID
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: scriptrecord.ScriptSetOwnerPrefix(
			record.EnvironmentID,
			active.Record.GenerationID,
		), Limit: 65, Revision: active.ReadRevision,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if page == nil || page.ReadRevision != active.ReadRevision || len(page.Values) >= 64 {
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Environment exceeds the 64 Script limit")
	}
	value, err := scriptrecord.EncodeRecord(record)
	if err != nil {
		return nil, nil, nil, err
	}
	bodyGeneration, err := scriptrecord.NewScriptBodyGeneration(record)
	if err != nil {
		clear(value)
		return nil, nil, nil, err
	}
	bodyValue, err := scriptrecord.EncodeScriptBodyGeneration(bodyGeneration)
	if err != nil {
		clear(value)
		return nil, nil, nil, err
	}
	locatorValue, err := scriptrecord.EncodeScriptLocator(
		scriptrecord.LocatorRecord{ScriptID: record.Desired.ID, EnvironmentID: record.EnvironmentID},
	)
	if err != nil {
		clear(value)
		clear(bodyValue)
		return nil, nil, nil, err
	}
	activeValue, err := scriptrecord.EncodeScriptSetGeneration(active.Record)
	if err != nil {
		clear(value)
		clear(bodyValue)
		clear(locatorValue)
		return nil, nil, nil, err
	}
	conditions := scriptWriteConditions(environment, project, target, record, nil, active, 0, 0)
	conditions = append(conditions, etcdstore.Condition{Key: scriptrecord.ScriptSetBodyGenerationKey(
		record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID, record.ActiveGeneration,
	)})
	mutations := []etcdstore.Mutation{
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptrecord.ScriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID),
			Value: value,
		},
		{
			Type: etcdstore.MutationPut,
			Key: scriptrecord.ScriptSetBodyGenerationKey(
				record.EnvironmentID,
				record.ScriptSetGeneration,
				record.Desired.ID,
				record.ActiveGeneration,
			),
			Value: bodyValue,
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptrecord.ScriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID),
			Value: []byte(record.Desired.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptrecord.ScriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
			Value: []byte(record.Desired.ID),
		},
		{Type: etcdstore.MutationPut, Key: scriptrecord.ScriptLocatorKey(record.Desired.ID), Value: locatorValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptrecord.ScriptEnvironmentLocatorKey(record.EnvironmentID, record.Desired.ID),
			Value: []byte(record.Desired.ID),
		},
		{Type: etcdstore.MutationPut, Key: scriptrecord.ScriptSetActiveKey(record.EnvironmentID), Value: activeValue},
	}
	classify := func(_ int64, values []*etcdstore.KeyValue) error {
		return classifyScriptWriteConflict(
			values,
			environment,
			project,
			target,
			record,
			0,
			scriptWriteConflictExtras{bodyGeneration: true},
		)
	}
	return conditions, mutations, classify, nil
}
