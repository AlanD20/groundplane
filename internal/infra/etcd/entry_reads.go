package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
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
	return recordquery.Get(
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
	return recordquery.ListIndex(
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
