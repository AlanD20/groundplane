package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// omittedEntryRecord distinguishes generated metadata from an unmaterialized
// declaration. Missing mutable rows never authorize deleting acknowledged data.
func (repository *EnvironmentBlueprintRepository) omittedEntryRecord(ctx context.Context,
	work workqueue.View, unit blueprintunits.Unit, snapshot blueprintunits.Snapshot,
	owned environmentprojection.EnvironmentOwnedIdentities, values []*keyvalue.KeyValue,
) (entries.Record, []keyvalue.Condition, error) {
	if values[4] != nil {
		record, err := entries.DecodeRecord(values[4].Value)
		if err != nil || record.Entry.ID != unit.Target.ID || record.EnvironmentID != owned.EnvironmentID ||
			values[5] == nil || string(values[5].Value) != record.Entry.ID || values[6] == nil ||
			string(values[6].Value) != record.EnvironmentID {
			return entries.Record{}, nil, errs.New(errs.KindStateConflict, "Blueprint Entry omission owner changed")
		}
		return record, nil, nil
	}
	source, err := blueprints.ReadOmissionSource(
		ctx,
		repository.store,
		owned,
		ids.KindEnvEntry,
		unit.Target.ID,
		snapshot.ReadRevision,
	)
	if err != nil {
		return entries.Record{}, nil, err
	}
	guards, absent, err := blueprintunits.DeclarationAbsenceConditions(snapshot, unit.Target,
		source.Identity.BirthRevisionID, work.Item.Item.ID)
	if err != nil {
		return entries.Record{}, nil, err
	}
	if !absent || values[5] != nil || values[6] != nil && string(values[6].Value) != owned.EnvironmentID {
		return entries.Record{}, nil, errs.New(
			errs.KindStateConflict,
			"Missing Entry metadata has no unmaterialized declaration proof",
		)
	}
	for _, namespace := range []string{entryvalues.PlainPrefix, entryvalues.SecretPrefix} {
		prefix := namespace + unit.Target.ID + "/"
		page, err := repository.store.Range(
			ctx,
			keyvalue.RangeRequest{Prefix: prefix, Limit: 1, Revision: snapshot.ReadRevision},
		)
		if err != nil {
			return entries.Record{}, nil, err
		}
		if page == nil || page.ReadRevision != snapshot.ReadRevision {
			return entries.Record{}, nil, errs.New(errs.KindInternal, "Unmaterialized Entry value read is incomplete")
		}
		keyvalue.ClearRangeValues(page.Values)
		if len(page.Values) != 0 || page.More {
			return entries.Record{}, nil, errs.New(
				errs.KindStateConflict,
				"Unmaterialized Entry still has retained values",
			)
		}
		guards = append(guards, keyvalue.Condition{Key: prefix, Prefix: true})
	}
	records, err := environmentprojection.AuthoredEntryRecords(source.Desired, source.Identities)
	if err != nil {
		return entries.Record{}, nil, err
	}
	for _, record := range records {
		if record.Entry.ID == unit.Target.ID {
			return record, append(guards, source.Conditions...), nil
		}
	}
	return entries.Record{}, nil, errs.New(
		errs.KindStateConflict,
		"Removed Entry is not declared in its retained source",
	)
}
