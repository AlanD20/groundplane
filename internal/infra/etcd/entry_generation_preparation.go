package etcd

import (
	"bytes"
	"github.com/AlanD20/groundplane/internal/core"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func prepareEntryGeneration(
	record entryrecord.Record,
	generation EntryValueGeneration,
) (string, []byte, error) {
	if (generation.Plain == nil) == (generation.Secret == nil) {
		return "", nil, errs.New(
			errs.KindValidationFailed,
			"Entry mutation must carry exactly one plain or secret value generation",
		)
	}
	if generation.Plain != nil {
		if record.Entry.Secret || generation.Plain.EnvironmentID != record.EnvironmentID ||
			generation.Plain.EntryID != record.Entry.ID ||
			generation.Plain.GenerationID != record.CurrentValueGenerationID {
			return "", nil, errs.New(errs.KindValidationFailed, "Entry plain value generation does not match metadata")
		}
		if record.Entry.Source.Kind == core.SourceLiteral &&
			!bytes.Equal(generation.Plain.Content, []byte(record.Entry.Source.Literal)) {
			return "", nil, errs.New(errs.KindValidationFailed, "Entry plain literal does not match its generation")
		}
		value, err := entryvalues.EncodePlain(*generation.Plain)
		return entryvalues.PlainKey(record.Entry.ID, record.CurrentValueGenerationID), value, err
	}
	if !record.Entry.Secret || generation.Secret.EnvironmentID != record.EnvironmentID ||
		generation.Secret.EntryID != record.Entry.ID ||
		generation.Secret.GenerationID != record.CurrentValueGenerationID {
		return "", nil, errs.New(errs.KindValidationFailed, "Entry secret value generation does not match metadata")
	}
	value, err := entryvalues.EncodeSecret(*generation.Secret)
	return entryvalues.SecretKey(record.Entry.ID, record.CurrentValueGenerationID), value, err
}
