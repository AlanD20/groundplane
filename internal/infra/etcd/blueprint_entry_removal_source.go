package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BlueprintEntryRemovalSource struct {
	Generation    uint64
	Configuration EntryRemovalConfiguration
	Projection    environmentprojection.EnvironmentComposeProjection
	Removal       *entrywork.BlueprintRemoval
	Conditions    []keyvalue.Condition
}

// ReadBlueprintEntryRemoval captures acknowledged files, not new desired
// values. Unrelated pending Attach facts cannot prevent retiring these sources.
func (repository *EnvironmentBlueprintRepository) ReadBlueprintEntryRemoval(ctx context.Context,
	work workqueue.View, unit blueprintunits.Unit,
) (BlueprintEntryRemovalSource, error) {
	task := work.Task.Task
	if task.Action != workexecution.ActionApply || unit.Target.Kind != ids.KindEnvEntry || !unit.Removal ||
		work.Item.Item.Operation != workexecution.EntryConfiguration || work.Item.Item.TargetID != unit.Target.ID ||
		work.ReadRevision <= 0 {
		return BlueprintEntryRemovalSource{}, errs.New(
			errs.KindValidationFailed,
			"Entry removal requires native Apply work",
		)
	}
	environmentID := task.Scope.EnvironmentID
	desired, found, err := blueprints.ReadDesiredInputRevision(
		ctx,
		repository.store,
		environmentID,
		task.ID,
		work.ReadRevision,
	)
	if err != nil {
		return BlueprintEntryRemovalSource{}, err
	}
	if !found {
		return BlueprintEntryRemovalSource{}, errs.New(
			errs.KindStateConflict,
			"Entry removal desired input is unavailable",
		)
	}
	owned, found, err := blueprints.ReadOwnedIdentitiesRevision(
		ctx,
		repository.store,
		environmentID,
		task.ID,
		work.ReadRevision,
	)
	if err != nil {
		return BlueprintEntryRemovalSource{}, err
	}
	if !found {
		return BlueprintEntryRemovalSource{}, errs.New(
			errs.KindStateConflict,
			"Entry removal authored identities are unavailable",
		)
	}
	for _, identity := range owned.Record.Entries {
		if identity.ID == unit.Target.ID {
			return BlueprintEntryRemovalSource{}, errs.New(
				errs.KindStateConflict,
				"Entry is still declared by this Apply",
			)
		}
	}
	declaration, err := blueprints.ReadOmissionSource(
		ctx,
		repository.store,
		owned.Record,
		ids.KindEnvEntry,
		unit.Target.ID,
		work.ReadRevision,
	)
	if err != nil {
		return BlueprintEntryRemovalSource{}, err
	}
	configuration, err := repository.ReadEntryRemovalConfiguration(ctx, environmentID, work.ReadRevision)
	if err != nil {
		return BlueprintEntryRemovalSource{}, err
	}
	result := BlueprintEntryRemovalSource{Generation: desired.Record.RenderGeneration, Configuration: configuration,
		Conditions: append(
			declaration.Conditions,
			keyvalue.Condition{
				Key:         blueprints.EnvironmentBlueprintRootKey(environmentID, task.ID),
				ModRevision: desired.Revision,
			},
			keyvalue.Condition{
				Key:         blueprints.EnvironmentBlueprintOwnedIdentitiesKey(environmentID, task.ID),
				ModRevision: owned.Revision,
			},
			keyvalue.Condition{
				Key:         runtimeconfiguration.HeadKey(environmentID),
				ModRevision: configuration.HeadRevision,
			},
		)}
	if !configuration.Files.ReferencesEntry(unit.Target.ID) {
		return result, nil
	}
	keys := []string{entries.RecordKey(unit.Target.ID), entries.EntryOwnerKey(environmentID, unit.Target.ID),
		entries.BlueprintEntryEnvironmentPrefix + unit.Target.ID, environmentprojection.EnvironmentComposeProjectionStorageKey(environmentID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: work.ReadRevision})
	if err != nil {
		return BlueprintEntryRemovalSource{}, err
	}
	if read == nil || read.ReadRevision != work.ReadRevision || len(read.Values) != len(keys) {
		return BlueprintEntryRemovalSource{}, errs.New(
			errs.KindInternal,
			"Entry file cleanup source read is incomplete",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	for index, key := range keys {
		value := read.Values[index]
		if value == nil || value.Key != key || value.ModRevision <= 0 {
			return BlueprintEntryRemovalSource{}, errs.New(
				errs.KindStateConflict,
				"Acknowledged Entry files have no matching metadata source",
			)
		}
		result.Conditions = append(result.Conditions, keyvalue.Condition{Key: key, ModRevision: value.ModRevision})
	}
	record, err := entries.DecodeRecord(read.Values[0].Value)
	if err != nil || record.Entry.ID != unit.Target.ID || record.EnvironmentID != environmentID ||
		string(read.Values[1].Value) != unit.Target.ID || string(read.Values[2].Value) != environmentID {
		return BlueprintEntryRemovalSource{}, errs.New(
			errs.KindStateConflict,
			"Entry file cleanup metadata owner changed",
		)
	}
	projection, err := environmentprojection.DecodeEnvironmentComposeProjectionStorage(read.Values[3].Value)
	if err != nil || projection.EnvironmentID != environmentID || projection.RenderGeneration >= result.Generation {
		return BlueprintEntryRemovalSource{}, errs.New(
			errs.KindStateConflict,
			"Entry cleanup has no acknowledged predecessor projection",
		)
	}
	result.Projection = projection
	result.Removal = &entrywork.BlueprintRemoval{
		EntryID:           unit.Target.ID,
		ValueGenerationID: record.CurrentValueGenerationID,
		RecordRevision:    read.Values[0].ModRevision,
		OwnerRevision:     read.Values[1].ModRevision,
		BindingRevision:   read.Values[2].ModRevision,
	}
	return result, nil
}
