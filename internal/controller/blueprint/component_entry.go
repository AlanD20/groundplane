package blueprint

import (
	"bytes"
	"context"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/core"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) componentEntryReference(
	ctx context.Context,
	environmentID, entryID string,
	entries []entryrecord.Record,
) (materializationrecord.EntryValueReference, error) {
	for _, record := range entries {
		if record.Entry.ID != entryID {
			continue
		}
		if record.EnvironmentID != environmentID {
			return materializationrecord.EntryValueReference{}, errs.New(
				errs.KindScopeUnauthorized,
				"Component token Entry belongs to another Environment",
			)
		}
		if _, err := (core.Environment{Entries: []core.EnvEntry{record.Entry}}).TunnelTokenEntry(record.Entry.Key); err != nil {
			return materializationrecord.EntryValueReference{}, errs.Wrap(errs.KindScopeUnauthorized, err)
		}
		reference := materializationrecord.EntryValueReference{
			EntryID:           entryID,
			ValueGenerationID: record.CurrentValueGenerationID,
			Storage:           materializationrecord.EntryValueStorageSecret,
		}
		value, err := service.materials.ResolveTaskMaterializationSource(
			ctx,
			environmentID,
			materializationrecord.Source{Kind: materializationrecord.SourceEntryValue, EntryValue: &reference},
		)
		defer clear(value)
		if err != nil {
			return materializationrecord.EntryValueReference{}, err
		}
		if len(bytes.TrimSpace(value)) == 0 {
			return materializationrecord.EntryValueReference{}, errs.New(
				errs.KindValidationFailed,
				"Cloudflare Tunnel token is empty; fill its Environment Configuration value before enabling",
			)
		}
		return reference, nil
	}
	return materializationrecord.EntryValueReference{}, errs.New(
		errs.KindValidationFailed,
		"Component token Entry is unavailable",
	)
}
