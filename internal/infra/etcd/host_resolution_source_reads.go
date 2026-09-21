package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/netip"
	"sort"
	"strings"
)

type scannedRoute struct {
	record routerecord.Record
}

func (repository *TaskRepository) scanRoutesAtRevision(ctx context.Context, revision int64) ([]scannedRoute, error) {
	result := make([]scannedRoute, 0)
	start := ""
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: projectionrecord.EnvironmentComposeProjectionPrefix, StartExclusive: start,
			Limit: etcdstore.MaximumPageLimit, Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if page == nil || page.ReadRevision != revision {
			return nil, errs.New(errs.KindInternal, "Route scan did not preserve its fixed revision")
		}
		for _, value := range page.Values {
			if !strings.HasPrefix(value.Key, projectionrecord.EnvironmentComposeProjectionPrefix) {
				recordquery.ClearRangeKeyValues(page.Values)
				return nil, errs.New(errs.KindInternal, "Applied Environment projection scan contains an invalid key")
			}
			environmentID := strings.TrimPrefix(value.Key, projectionrecord.EnvironmentComposeProjectionPrefix)
			if strings.Contains(environmentID, "/") ||
				recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil {
				recordquery.ClearRangeKeyValues(page.Values)
				return nil, recordcodec.CorruptRecord()
			}
			projection, decodeErr := projectionrecord.DecodeEnvironmentComposeProjectionStorage(value.Value)
			if decodeErr != nil || projection.EnvironmentID != environmentID {
				recordquery.ClearRangeKeyValues(page.Values)
				return nil, projectionrecord.CorruptEnvironmentComposeProjection()
			}
			versioned := etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
				Record: projection, Revision: value.ModRevision, ReadRevision: page.ReadRevision,
			}
			for _, desired := range projection.DesiredRoutes {
				record, joinErr := projectionrecord.ReadRoute(ctx, repository.store, versioned, desired)
				if joinErr != nil {
					recordquery.ClearRangeKeyValues(page.Values)
					return nil, joinErr
				}
				result = append(result, scannedRoute{record: record.Record})
			}
		}
		if !page.More {
			recordquery.ClearRangeKeyValues(page.Values)
			return result, nil
		}
		if len(page.Values) == 0 {
			return nil, errs.New(errs.KindInternal, "Route scan did not advance")
		}
		start = page.Values[len(page.Values)-1].Key
		recordquery.ClearRangeKeyValues(page.Values)
	}
}

func (repository *TaskRepository) hostResolutionComponents(
	ctx context.Context,
	idsByComponent map[string]struct{},
	revision int64,
) (map[string]componentrecord.Record, []etcdstore.Condition, error) {
	keys := make([]string, 0, len(idsByComponent))
	for id := range idsByComponent {
		keys = append(keys, componentrecord.RecordKey(id))
	}
	sort.Strings(keys)
	result := make(map[string]componentrecord.Record, len(keys))
	if len(keys) == 0 {
		return result, nil, nil
	}
	stateKeys := append([]string{componentrecord.WriteFenceKey}, keys...)
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: stateKeys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if state == nil || state.ReadRevision != revision || len(state.Values) != len(stateKeys) {
		return nil, nil, errs.New(errs.KindInternal, "host-resolution Component read is incomplete")
	}
	fence := etcdstore.Condition{Key: componentrecord.WriteFenceKey}
	if state.Values[0] != nil {
		fence.ModRevision = state.Values[0].ModRevision
	}
	for index, value := range state.Values[1:] {
		if value == nil {
			return nil, nil, errs.New(errs.KindStateConflict, "host-resolution provider Component is missing")
		}
		record, decodeErr := componentrecord.DecodeRecord(value.Value)
		if decodeErr != nil {
			return nil, nil, decodeErr
		}
		id := strings.TrimPrefix(keys[index], componentrecord.RecordPrefix)
		if record.Desired.ID != id {
			return nil, nil, recordcodec.CorruptRecord()
		}
		result[id] = record
	}
	return result, []etcdstore.Condition{fence}, nil
}

func validHostResolutionProvider(record componentrecord.Record) bool {
	if record.Desired.Owner != core.ComponentOwnerEnvironment ||
		record.Desired.Kind != core.ComponentKindIngressCaddy || !record.Desired.Enabled || !record.Runtime.Healthy ||
		len(
			record.Runtime.GeneratedServices,
		) != 1 || ids.Validate(ids.KindService, record.Runtime.GeneratedServices[0]) != nil {
		return false
	}
	address, err := netip.ParseAddr(record.Runtime.PinnedIPv4)
	return err == nil && address.Is4() && !address.Is4In6() && !address.IsUnspecified() &&
		!address.IsMulticast() && address.String() == record.Runtime.PinnedIPv4
}

func appendHostResolutionCondition(
	conditions []etcdstore.Condition,
	candidate etcdstore.Condition,
) []etcdstore.Condition {
	for _, existing := range conditions {
		if existing.Key != candidate.Key {
			continue
		}
		return conditions
	}
	return append(conditions, candidate)
}

func (repository *TaskRepository) routeAtRevision(
	ctx context.Context,
	routeID string,
	revision int64,
) (routerecord.Record, error) {
	routes, err := repository.scanRoutesAtRevision(ctx, revision)
	if err != nil {
		return routerecord.Record{}, err
	}
	for _, route := range routes {
		if route.record.Desired.ID == routeID {
			return route.record, nil
		}
	}
	return routerecord.Record{}, errs.New(errs.KindStateConflict, "host-resolution Route changed during reconciliation")
}
