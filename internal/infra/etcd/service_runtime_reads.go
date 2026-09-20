package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// LoadAcknowledgedServiceRuntimesAtRevision returns only existing acknowledged
// runtime at one fixed read revision. It never reconstructs missing authority
// from immutable Release input.
func (ledger *ReleaseLedger) LoadAcknowledgedServiceRuntimesAtRevision(
	ctx context.Context, environmentID string, serviceIDs []string, revision int64,
) ([]etcdstore.Versioned[serviceruntimerecord.Record], error) {
	if ledger == nil || ledger.store == nil || ctx == nil || ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		revision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "acknowledged Service runtime request is invalid")
	}
	keys := make([]string, len(serviceIDs))
	for index, serviceID := range serviceIDs {
		if ids.Validate(ids.KindService, serviceID) != nil || index > 0 && serviceIDs[index-1] >= serviceID {
			return nil, errs.New(errs.KindValidationFailed, "acknowledged Service runtime Services are invalid")
		}
		keys[index] = serviceruntimerecord.Key(serviceID)
	}
	if len(keys) == 0 {
		return []etcdstore.Versioned[serviceruntimerecord.Record]{}, nil
	}
	read, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, errs.New(errs.KindInternal, "acknowledged Service runtime read is incomplete")
	}
	result := make([]etcdstore.Versioned[serviceruntimerecord.Record], len(keys))
	defer clearKeyValues(read.Values)
	for index, value := range read.Values {
		if value == nil {
			return nil, errs.New(errs.KindStateConflict, "running Service has no acknowledged runtime")
		}
		record, decodeErr := decodeAcknowledgedServiceRuntime(value.Value, environmentID, serviceIDs[index])
		if decodeErr != nil {
			return nil, decodeErr
		}
		record.Runtime.ProxyConfigSHA256 = slices.Clone(record.Runtime.ProxyConfigSHA256)
		record.Runtime.CurrentArtifact = slices.Clone(record.Runtime.CurrentArtifact)
		record.Runtime.RetainedPriorArtifact = slices.Clone(record.Runtime.RetainedPriorArtifact)
		result[index] = etcdstore.Versioned[serviceruntimerecord.Record]{
			Record: record, Revision: value.ModRevision, ReadRevision: read.ReadRevision,
		}
	}
	return result, nil
}
