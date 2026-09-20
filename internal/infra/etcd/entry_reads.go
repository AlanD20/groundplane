package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *EntryRepository) GetEntry(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[entryrecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvEntry, id); err != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		entryrecord.RecordKey(id),
		id,
		errs.KindEntryNotFound,
		entryrecord.DecodeRecord,
		func(record entryrecord.Record) string { return record.Entry.ID },
	)
}

func (repository *EntryRepository) ListEntries(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[entryrecord.Record], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[entryrecord.Record]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"entries",
		"environment",
		environmentID,
		entryOwnerCollectionPrefix(environmentID),
		entryrecord.RecordKey,
		ids.KindEnvEntry,
		request,
		entryrecord.DecodeRecord,
		func(record entryrecord.Record) string { return record.Entry.ID },
		func(record entryrecord.Record) bool { return record.EnvironmentID == environmentID },
	)
}
