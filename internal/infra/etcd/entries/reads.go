package entries

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) GetEntry(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvEntry, id); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	return recordquery.Get(
		ctx,
		repository.store,
		RecordKey(id),
		id,
		errs.KindEntryNotFound,
		DecodeRecord,
		func(record Record) string { return record.Entry.ID },
	)
}

func (repository *Repository) ListEntries(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[Record], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[Record]{}, err
	}
	return recordquery.ListIndex(
		ctx,
		repository.store,
		"entries",
		"environment",
		environmentID,
		EntryOwnerCollectionPrefix(environmentID),
		RecordKey,
		ids.KindEnvEntry,
		request,
		DecodeRecord,
		func(record Record) string { return record.Entry.ID },
		func(record Record) bool { return record.EnvironmentID == environmentID },
	)
}
