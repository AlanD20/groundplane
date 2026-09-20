package operations

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/pkg/errs"
	"sort"
)

type entryBulkChange struct {
	desired  core.EnvEntry
	record   entryrecord.Record
	previous *entryrecord.Record
}

type entryBulkCandidate struct {
	entries  []entryrecord.Record
	changes  []entryBulkChange
	previous []entryrecord.Record
}

func buildEntryBulkCandidate(
	current etcd.EnvironmentComposeProjection,
	input entryBulkUpsertInput,
	revisionID string,
) (entryBulkCandidate, error) {
	byKey := make(map[string]entryrecord.Record)
	for _, record := range current.Entries {
		if record.Entry.Kind != core.EntryKindEnv {
			continue
		}
		if _, duplicate := byKey[record.Entry.Key]; duplicate {
			return entryBulkCandidate{}, errs.New(
				errs.KindInternal,
				"Environment Entry projection has a duplicated env key",
			)
		}
		byKey[record.Entry.Key] = record
	}
	changes := make([]entryBulkChange, 0, len(input.entries))
	previous := make([]entryrecord.Record, 0, len(input.entries))
	replaced := make(map[string]struct{}, len(input.entries))
	for _, item := range input.entries {
		desired := core.EnvEntry{
			Kind: core.EntryKindEnv, Key: item.key,
			Source:   core.EntrySource{Kind: core.SourceLiteral, Literal: item.value},
			Exposure: append([]string(nil), input.exposure...), Secret: input.secret,
		}
		var prior *entryrecord.Record
		if existing, found := byKey[item.key]; found {
			if existing.Entry.Secret != input.secret {
				return entryBulkCandidate{}, errs.Newf(
					errs.KindValidationFailed, "Entry %q already uses the %s storage class", item.key,
					entryStorageClass(existing.Entry.Secret),
				)
			}
			value := existing
			prior = &value
			previous = append(previous, value)
			desired.ID = existing.Entry.ID
			replaced[existing.Entry.ID] = struct{}{}
		} else {
			entryID, err := deriveEntryBulkID(ids.KindEnvEntry, revisionID, "entry/"+item.key)
			if err != nil {
				return entryBulkCandidate{}, err
			}
			desired.ID = entryID
		}
		generationID, err := deriveEntryBulkID(ids.KindConfig, revisionID, "value/"+item.key)
		if err != nil {
			return entryBulkCandidate{}, err
		}
		persisted := desired
		if persisted.Secret {
			persisted.Source.Literal = ""
		}
		record, err := entryrecord.NewRecord(current.EnvironmentID, persisted, generationID)
		if err != nil {
			return entryBulkCandidate{}, err
		}
		if prior != nil {
			record.BlueprintKey = prior.BlueprintKey
		}
		changes = append(changes, entryBulkChange{desired: desired, record: record, previous: prior})
	}
	entries := make([]entryrecord.Record, 0, len(current.Entries)+len(changes))
	for _, record := range current.Entries {
		if _, drop := replaced[record.Entry.ID]; !drop {
			entries = append(entries, record)
		}
	}
	for _, change := range changes {
		entries = append(entries, change.record)
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].Entry.ID < entries[right].Entry.ID })
	sort.Slice(previous, func(left, right int) bool { return previous[left].Entry.ID < previous[right].Entry.ID })
	return entryBulkCandidate{entries: entries, changes: changes, previous: previous}, nil
}

func deriveEntryBulkID(kind ids.Kind, revisionID string, purpose string) (string, error) {
	createdAt, err := ids.Timestamp(ids.KindTask, revisionID)
	if err != nil {
		return "", errs.New(errs.KindInternal, "Entry bulk upsert revision identity is invalid")
	}
	return ids.DeriveAt(kind, createdAt, revisionID, "entry-bulk/"+purpose), nil
}

func entryStorageClass(secret bool) string {
	if secret {
		return "secret"
	}
	return "plain"
}
