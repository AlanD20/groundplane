package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadRouteWorkComponent captures the router definition at the same snapshot as
// its applied runtime. The revision belongs to the Component key, not the
// Environment desired head.
func (repository *HierarchyRepository) ReadRouteWorkComponent(ctx context.Context, expected components.Record, revision int64) (keyvalue.Versioned[components.Record], error) {
	if ctx == nil || repository == nil || revision <= 0 || components.ValidateRecord(expected) != nil || !expected.Desired.Enabled {
		return keyvalue.Versioned[components.Record]{}, errs.New(errs.KindValidationFailed, "Route requires its enabled router definition")
	}
	key := components.RecordKey(expected.Desired.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[components.Record]{}, err
	}
	if read == nil {
		return keyvalue.Versioned[components.Record]{}, errs.New(errs.KindStateConflict, "Route router snapshot is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > revision {
		return keyvalue.Versioned[components.Record]{}, errs.New(errs.KindStateConflict, "Route router definition is unavailable at the captured snapshot")
	}
	record, err := components.DecodeRecord(read.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[components.Record]{}, err
	}
	if !components.EqualRecord(record, expected) {
		return keyvalue.Versioned[components.Record]{}, errs.New(errs.KindStateConflict, "Route router definition changed from its captured desired state")
	}
	return keyvalue.Versioned[components.Record]{Record: record, Revision: read.Values[0].ModRevision, ReadRevision: revision}, nil
}
