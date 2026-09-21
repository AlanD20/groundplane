package environmentqueries

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func FindZoneAtRevision(
	ctx context.Context,
	store snapshotReader,
	zoneID string,
	revision int64,
) (etcdstore.Versioned[zonerecord.Record], error) {
	start := ""
	fixedRevision := revision
	var matched *etcdstore.Versioned[zonerecord.Record]
	for {
		page, err := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: EnvironmentDesiredHeadScanPrefix, StartExclusive: start, Limit: 200, Revision: fixedRevision,
		})
		if err != nil {
			return etcdstore.Versioned[zonerecord.Record]{}, err
		}
		if page == nil || page.ReadRevision <= 0 {
			return etcdstore.Versioned[zonerecord.Record]{}, errs.New(errs.KindInternal, "Environment desired head scan is invalid")
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
				strings.TrimPrefix(value.Key, EnvironmentDesiredHeadScanPrefix), "/current",
			)
			if strings.Contains(environmentID, "/") || ids.Validate(ids.KindEnvironment, environmentID) != nil {
				return etcdstore.Versioned[zonerecord.Record]{}, projectionrecord.CorruptEnvironmentComposeProjection()
			}
			projection, found, projectionErr := blueprints.ReadCurrentProjection(
				ctx, store, environmentID, fixedRevision,
			)
			if projectionErr != nil {
				return etcdstore.Versioned[zonerecord.Record]{}, projectionErr
			}
			if !found {
				continue
			}
			for _, desired := range projection.Record.DesiredZones {
				if desired.Desired.ID != zoneID {
					continue
				}
				if matched != nil {
					return etcdstore.Versioned[zonerecord.Record]{}, projectionrecord.CorruptEnvironmentComposeProjection()
				}
				joined, joinErr := JoinZone(projection, desired)
				if joinErr != nil {
					return etcdstore.Versioned[zonerecord.Record]{}, joinErr
				}
				matched = &joined
			}
		}
		if !page.More {
			break
		}
		if len(page.Values) == 0 {
			return etcdstore.Versioned[zonerecord.Record]{}, errs.New(errs.KindInternal, "Environment desired head scan did not advance")
		}
	}
	if matched == nil {
		return etcdstore.Versioned[zonerecord.Record]{}, errs.New(errs.KindZoneNotFound, "Zone was not found")
	}
	return *matched, nil
}
