package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *EnvironmentBlueprintRepository) validateEntryMetadataRecords(
	ctx context.Context, request EntryMetadataPublication,
) error {
	changed := make(map[string]bool, len(request.Changed))
	for _, id := range request.Changed {
		if ids.Validate(ids.KindEnvEntry, id) != nil || changed[id] {
			return errs.New(errs.KindValidationFailed, "Entry mutation targets are invalid or duplicated")
		}
		changed[id] = true
	}
	previous := make(map[string]entries.Record, len(request.Source.Entries))
	for _, record := range request.Source.Entries {
		previous[record.Entry.ID] = record
	}
	seen := make(map[string]bool, len(request.Records))
	for _, record := range request.Records {
		id := record.Entry.ID
		if seen[id] || record.EnvironmentID != request.Task.Scope.EnvironmentID ||
			entries.ValidateRecord(record) != nil {
			return errs.New(errs.KindValidationFailed, "Entry metadata identity is invalid")
		}
		seen[id] = true
		prior, exists := previous[id]
		if !changed[id] {
			if !exists || !entries.EqualRecord(prior, record) {
				return errs.New(errs.KindStateConflict, "Entry mutation changed an unrelated declaration")
			}
			continue
		}
		if exists {
			comparable := prior
			comparable.Entry.Source, comparable.Entry.Exposure = record.Entry.Source, record.Entry.Exposure
			comparable.CurrentValueGenerationID = record.CurrentValueGenerationID
			if !entries.EqualRecord(comparable, record) {
				return errs.New(errs.KindValidationFailed, "Entry mutation changed an immutable identity")
			}
		}
		available, err := repository.entryMetadataGenerationExists(ctx, record)
		if err != nil {
			return err
		}
		if !available {
			return errs.New(errs.KindStateConflict, "Entry selected value generation is unavailable")
		}
	}
	for id := range previous {
		if !seen[id] && !changed[id] {
			return errs.New(errs.KindStateConflict, "Entry mutation omitted an unrelated declaration")
		}
	}
	return nil
}

func (repository *EnvironmentBlueprintRepository) entryMetadataGenerationExists(
	ctx context.Context, record entries.Record,
) (bool, error) {
	key := entryvalues.PlainKey(record.Entry.ID, record.CurrentValueGenerationID)
	if record.Entry.Secret {
		key = entryvalues.SecretKey(record.Entry.ID, record.CurrentValueGenerationID)
	}
	read, err := repository.store.Get(ctx, key)
	if err != nil {
		return false, err
	}
	if read == nil {
		return false, errs.New(errs.KindInternal, "Entry value generation observation is incomplete")
	}
	if read.Entry == nil {
		return false, nil
	}
	defer clear(read.Entry.Value)
	if record.Entry.Secret {
		value, err := entryvalues.DecodeSecret(read.Entry.Value)
		defer clear(value.Ciphertext)
		return err == nil && value.EnvironmentID == record.EnvironmentID && value.EntryID == record.Entry.ID &&
			value.GenerationID == record.CurrentValueGenerationID, err
	}
	value, err := entryvalues.DecodePlain(read.Entry.Value)
	defer clear(value.Content)
	return err == nil && value.EnvironmentID == record.EnvironmentID && value.EntryID == record.Entry.ID &&
		value.GenerationID == record.CurrentValueGenerationID, err
}
