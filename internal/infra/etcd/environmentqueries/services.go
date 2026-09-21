package environmentqueries

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const EnvironmentDesiredHeadScanPrefix = "/v1/records/environment-blueprints/"

func FindServiceAtRevision(
	ctx context.Context,
	store snapshotReader,
	serviceID string,
	revision int64,
) (etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	start := ""
	fixedRevision := revision
	var matched *etcdstore.Versioned[servicerecord.ServiceRecord]
	for {
		page, err := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: EnvironmentDesiredHeadScanPrefix, StartExclusive: start, Limit: 200, Revision: fixedRevision,
		})
		if err != nil {
			return etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
		}
		if page == nil || page.ReadRevision <= 0 {
			return etcdstore.Versioned[servicerecord.ServiceRecord]{}, errs.New(
				errs.KindInternal,
				"Environment desired head scan is invalid",
			)
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
				strings.TrimPrefix(value.Key, EnvironmentDesiredHeadScanPrefix),
				"/current",
			)
			if strings.Contains(environmentID, "/") || ids.Validate(ids.KindEnvironment, environmentID) != nil {
				return etcdstore.Versioned[servicerecord.ServiceRecord]{}, projectionrecord.CorruptEnvironmentComposeProjection()
			}
			projection, found, projectionErr := blueprints.ReadCurrentProjection(
				ctx,
				store,
				environmentID,
				fixedRevision,
			)
			if projectionErr != nil {
				return etcdstore.Versioned[servicerecord.ServiceRecord]{}, projectionErr
			}
			if !found {
				continue
			}
			for _, desired := range projection.Record.DesiredServices {
				if desired.Desired.ID != serviceID {
					continue
				}
				if IsComponentService(projection.Record.Components, serviceID) {
					continue
				}
				if matched != nil {
					return etcdstore.Versioned[servicerecord.ServiceRecord]{}, projectionrecord.CorruptEnvironmentComposeProjection()
				}
				joined, joinErr := servicerecord.ReadJoined(
					ctx,
					store,
					servicerecord.DesiredSelection{
						Services:     projection.Record.DesiredServices,
						Revision:     projection.Revision,
						ReadRevision: projection.ReadRevision,
					},
					serviceID,
					blueprints.EnvironmentBlueprintHeadKey(environmentID),
				)
				if joinErr != nil {
					return etcdstore.Versioned[servicerecord.ServiceRecord]{}, joinErr
				}
				matched = &joined
			}
		}
		if !page.More {
			break
		}
		if len(page.Values) == 0 {
			return etcdstore.Versioned[servicerecord.ServiceRecord]{}, errs.New(
				errs.KindInternal,
				"Environment desired head scan did not advance",
			)
		}
	}
	if matched == nil {
		return etcdstore.Versioned[servicerecord.ServiceRecord]{}, errs.New(
			errs.KindServiceNotFound,
			"Service was not found",
		)
	}
	return *matched, nil
}

func OrdinaryServices(
	projection projectionrecord.EnvironmentComposeProjection,
) []servicerecord.EnvironmentServiceProjection {
	result := make([]servicerecord.EnvironmentServiceProjection, 0, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		if IsComponentService(projection.Components, service.Desired.ID) {
			continue
		}
		result = append(result, service)
	}
	return result
}

func IsComponentService(components []componentrecord.Record, serviceID string) bool {
	for _, component := range components {
		for _, generatedServiceID := range component.Runtime.GeneratedServices {
			if generatedServiceID == serviceID {
				return true
			}
		}
	}
	return false
}
