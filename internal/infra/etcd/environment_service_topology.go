package etcd

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const environmentDesiredHeadScanPrefix = "/v1/records/environment-blueprints/"

func (repository *ServiceRepository) GetService(
	ctx context.Context,
	serviceID string,
) (etcdstore.Versioned[ServiceRecord], error) {
	if err := validateContext(ctx); err != nil {
		return etcdstore.Versioned[ServiceRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindService, serviceID); err != nil {
		return etcdstore.Versioned[ServiceRecord]{}, err
	}
	return findServiceAtRevision(ctx, repository.store, serviceID, 0)
}

func (repository *ServiceRepository) GetServiceRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
	serviceID string,
) (etcdstore.Versioned[ServiceRecord], error) {
	hierarchy := &HierarchyRepository{store: repository.store}
	projection, found, err := hierarchy.GetEnvironmentComposeProjectionRevision(ctx, environmentID, revisionID)
	if err != nil {
		return etcdstore.Versioned[ServiceRecord]{}, err
	}
	if !found {
		return etcdstore.Versioned[ServiceRecord]{}, errs.New(errs.KindServiceNotFound, "Service was not found")
	}
	return joinEnvironmentService(ctx, repository.store, projection, serviceID,
		environmentBlueprintRootKey(environmentID, revisionID))
}

func (repository *ServiceRepository) GetServiceByName(
	ctx context.Context,
	environmentID string,
	name string,
) (etcdstore.Versioned[ServiceRecord], error) {
	if name == "" {
		return etcdstore.Versioned[ServiceRecord]{}, errs.New(errs.KindValidationFailed, "Service name is required")
	}
	projection, found, err := currentEnvironmentProjectionAtRevision(ctx, repository.store, environmentID, 0)
	if err != nil {
		return etcdstore.Versioned[ServiceRecord]{}, err
	}
	if !found {
		return etcdstore.Versioned[ServiceRecord]{}, errs.New(errs.KindServiceNotFound, "Service was not found")
	}
	for _, service := range projection.Record.DesiredServices {
		if service.Desired.Name == name &&
			!componentGeneratedService(projection.Record.Components, service.Desired.ID) {
			return joinEnvironmentService(ctx, repository.store, projection, service.Desired.ID,
				environmentBlueprintHeadKey(environmentID))
		}
	}
	return etcdstore.Versioned[ServiceRecord]{}, errs.New(errs.KindServiceNotFound, "Service was not found")
}

func (repository *ServiceRepository) ListServices(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[ServiceRecord], error) {
	if err := validateContext(ctx); err != nil {
		return etcdstore.Page[ServiceRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[ServiceRecord]{}, err
	}
	limit, revision, lastID, query, err := normalizePageRequest(
		request, "services", "environment", environmentID, "", ids.KindService,
	)
	if err != nil {
		return etcdstore.Page[ServiceRecord]{}, err
	}
	projection, found, err := currentEnvironmentProjectionAtRevision(ctx, repository.store, environmentID, revision)
	if err != nil {
		return etcdstore.Page[ServiceRecord]{}, err
	}
	if !found {
		return etcdstore.Page[ServiceRecord]{Items: []etcdstore.Versioned[ServiceRecord]{}, Revision: projection.ReadRevision}, nil
	}
	desired := ordinaryEnvironmentServices(projection.Record)
	sort.Slice(desired, func(left, right int) bool { return desired[left].Desired.ID < desired[right].Desired.ID })
	start := sort.Search(len(desired), func(index int) bool { return desired[index].Desired.ID > lastID })
	end := min(start+limit, len(desired))
	items := make([]etcdstore.Versioned[ServiceRecord], 0, end-start)
	for _, service := range desired[start:end] {
		joined, joinErr := joinEnvironmentService(ctx, repository.store, projection, service.Desired.ID,
			environmentBlueprintHeadKey(environmentID))
		if joinErr != nil {
			return etcdstore.Page[ServiceRecord]{}, joinErr
		}
		items = append(items, joined)
	}
	next := ""
	if end < len(desired) {
		next, err = recordcodec.EncodeCursor(recordcodec.Cursor{
			Version: recordcodec.CursorVersion, Revision: projection.ReadRevision,
			LastID: desired[end-1].Desired.ID, Query: query,
		})
		if err != nil {
			return etcdstore.Page[ServiceRecord]{}, err
		}
	}
	return etcdstore.Page[ServiceRecord]{Items: items, NextCursor: next, Revision: projection.ReadRevision}, nil
}

func currentEnvironmentProjectionAtRevision(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	revision int64,
) (etcdstore.Versioned[EnvironmentComposeProjection], bool, error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[EnvironmentComposeProjection]{}, false, err
	}
	hierarchy := &HierarchyRepository{store: store}
	return hierarchy.getEnvironmentComposeProjectionAtRevision(ctx, environmentID, revision)
}

func findServiceAtRevision(
	ctx context.Context,
	store hierarchyStore,
	serviceID string,
	revision int64,
) (etcdstore.Versioned[ServiceRecord], error) {
	start := ""
	fixedRevision := revision
	var matched *etcdstore.Versioned[ServiceRecord]
	for {
		page, err := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: environmentDesiredHeadScanPrefix, StartExclusive: start, Limit: 200, Revision: fixedRevision,
		})
		if err != nil {
			return etcdstore.Versioned[ServiceRecord]{}, err
		}
		if page == nil || page.ReadRevision <= 0 {
			return etcdstore.Versioned[ServiceRecord]{}, errs.New(errs.KindInternal, "Environment desired head scan is invalid")
		}
		if fixedRevision == 0 {
			fixedRevision = page.ReadRevision
		}
		for index := range page.Values {
			value := &page.Values[index]
			start = value.Key
			if !strings.HasSuffix(value.Key, "/current") {
				continue
			}
			environmentID := strings.TrimSuffix(
				strings.TrimPrefix(value.Key, environmentDesiredHeadScanPrefix),
				"/current",
			)
			if strings.Contains(environmentID, "/") || ids.Validate(ids.KindEnvironment, environmentID) != nil {
				return etcdstore.Versioned[ServiceRecord]{}, corruptEnvironmentComposeProjection()
			}
			projection, found, projectionErr := currentEnvironmentProjectionAtRevision(
				ctx,
				store,
				environmentID,
				fixedRevision,
			)
			if projectionErr != nil {
				return etcdstore.Versioned[ServiceRecord]{}, projectionErr
			}
			if !found {
				continue
			}
			for _, desired := range projection.Record.DesiredServices {
				if desired.Desired.ID != serviceID {
					continue
				}
				if componentGeneratedService(projection.Record.Components, serviceID) {
					continue
				}
				if matched != nil {
					return etcdstore.Versioned[ServiceRecord]{}, corruptEnvironmentComposeProjection()
				}
				joined, joinErr := joinEnvironmentService(ctx, store, projection, serviceID,
					environmentBlueprintHeadKey(environmentID))
				if joinErr != nil {
					return etcdstore.Versioned[ServiceRecord]{}, joinErr
				}
				matched = &joined
			}
		}
		if !page.More {
			break
		}
		if len(page.Values) == 0 {
			return etcdstore.Versioned[ServiceRecord]{}, errs.New(
				errs.KindInternal,
				"Environment desired head scan did not advance",
			)
		}
	}
	if matched == nil {
		return etcdstore.Versioned[ServiceRecord]{}, errs.New(errs.KindServiceNotFound, "Service was not found")
	}
	return *matched, nil
}

func ordinaryEnvironmentServices(projection EnvironmentComposeProjection) []EnvironmentServiceProjection {
	result := make([]EnvironmentServiceProjection, 0, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		if componentGeneratedService(projection.Components, service.Desired.ID) {
			continue
		}
		result = append(result, service)
	}
	return result
}

func componentGeneratedService(components []componentrecord.Record, serviceID string) bool {
	for _, component := range components {
		for _, generatedServiceID := range component.Runtime.GeneratedServices {
			if generatedServiceID == serviceID {
				return true
			}
		}
	}
	return false
}

func joinEnvironmentService(
	ctx context.Context,
	store hierarchyStore,
	projection etcdstore.Versioned[EnvironmentComposeProjection],
	serviceID string,
	desiredFenceKey string,
) (etcdstore.Versioned[ServiceRecord], error) {
	var desired *EnvironmentServiceProjection
	for index := range projection.Record.DesiredServices {
		candidate := &projection.Record.DesiredServices[index]
		if candidate.Desired.ID == serviceID {
			desired = candidate
			break
		}
	}
	if desired == nil {
		return etcdstore.Versioned[ServiceRecord]{}, errs.New(errs.KindServiceNotFound, "Service was not found")
	}
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{serviceRuntimeKey(serviceID)}, Revision: projection.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[ServiceRecord]{}, err
	}
	if read == nil || len(read.Values) != 1 || read.ReadRevision != projection.ReadRevision {
		return etcdstore.Versioned[ServiceRecord]{}, errs.New(errs.KindInternal, "Service runtime read is invalid")
	}
	runtime := core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning}
	runtimeRevision := int64(0)
	if read.Values[0] != nil {
		sidecar, decodeErr := decodeServiceRuntimeRecord(read.Values[0].Value)
		if decodeErr != nil || sidecar.EnvironmentID != desired.EnvironmentID || sidecar.ServiceID != serviceID ||
			sidecar.BackingNetworkID != desired.BackingNetworkID {
			return etcdstore.Versioned[ServiceRecord]{}, recordcodec.CorruptRecord()
		}
		runtime = sidecar.Runtime
		runtimeRevision = read.Values[0].ModRevision
	}
	record := ServiceRecord{
		EnvironmentID: desired.EnvironmentID, BackingNetworkID: desired.BackingNetworkID,
		Desired: desired.Desired, Runtime: runtime,
		desiredFenceKey: desiredFenceKey, runtimeRevision: runtimeRevision,
	}
	if err := validateServiceRecord(record); err != nil {
		return etcdstore.Versioned[ServiceRecord]{}, recordcodec.CorruptRecord()
	}
	return etcdstore.Versioned[ServiceRecord]{
		Record:       record,
		Revision:     projection.Revision,
		ReadRevision: projection.ReadRevision,
	}, nil
}
