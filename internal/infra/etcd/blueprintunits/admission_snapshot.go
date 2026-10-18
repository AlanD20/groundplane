package blueprintunits

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// LoadAdmission reads the resource ledger before accepting a new Apply. No head
// is valid for a first Apply only when no prior resource ledger exists. Missing
// runtime projection is irrelevant; this read never manufactures applied state.
func (repository *Repository) LoadAdmission(ctx context.Context, environmentID string, revision int64) (Snapshot, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return Snapshot{}, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || revision <= 0 {
		return Snapshot{}, invalidRecord()
	}
	key := blueprints.EnvironmentBlueprintHeadKey(environmentID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return Snapshot{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return Snapshot{}, corruptRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	if value := read.Values[0]; value != nil {
		if value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision {
			return Snapshot{}, corruptRecord()
		}
		return repository.LoadAtRevision(ctx, environmentID, revision)
	}
	page, err := repository.store.Range(ctx, keyvalue.RangeRequest{Prefix: recordPrefix + environmentID + "/", Limit: 1, Revision: revision})
	if err != nil {
		return Snapshot{}, err
	}
	if page == nil || page.ReadRevision != revision || len(page.Values) != 0 || page.More {
		return Snapshot{}, corruptRecord()
	}
	return Snapshot{EnvironmentID: environmentID, ReadRevision: revision}, nil
}
