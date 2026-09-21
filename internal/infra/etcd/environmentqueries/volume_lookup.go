package environmentqueries

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
)

// FindEnvironmentVolume resolves a stable Volume id from the sole published
// desired-state authority. The MVP deliberately prefers a bounded sequential
// head scan over a second synchronously writable identity authority.
func (repository *ProjectionReader) FindEnvironmentVolume(
	ctx context.Context,
	volumeID string,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], projectionrecord.EnvironmentVolumeIdentity, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, projectionrecord.EnvironmentVolumeIdentity{}, err
	}
	if err := recordcodec.ValidateID(ids.KindVolume, volumeID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, projectionrecord.EnvironmentVolumeIdentity{}, err
	}
	const headsPrefix = "/v1/records/environment-blueprints/"
	start := ""
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: headsPrefix, StartExclusive: start, Limit: 128,
		})
		if err != nil {
			return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, projectionrecord.EnvironmentVolumeIdentity{}, err
		}
		if page == nil {
			return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, projectionrecord.EnvironmentVolumeIdentity{}, errs.New(
				errs.KindInternal, "Environment desired-head scan is empty",
			)
		}
		for _, entry := range page.Values {
			start = entry.Key
			if !strings.HasSuffix(entry.Key, "/current") {
				continue
			}
			environmentID := strings.TrimSuffix(strings.TrimPrefix(entry.Key, headsPrefix), "/current")
			if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil {
				return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, projectionrecord.EnvironmentVolumeIdentity{}, projectionrecord.CorruptEnvironmentComposeProjection()
			}
			revisionID, err := idempotencyrecord.DecodeTaskReference(entry.Value)
			if err != nil {
				return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, projectionrecord.EnvironmentVolumeIdentity{}, projectionrecord.CorruptEnvironmentComposeProjection()
			}
			projection, found, err := repository.GetEnvironmentComposeProjectionRevision(ctx, environmentID, revisionID)
			if err != nil {
				return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, projectionrecord.EnvironmentVolumeIdentity{}, err
			}
			if !found {
				return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, projectionrecord.EnvironmentVolumeIdentity{}, projectionrecord.CorruptEnvironmentComposeProjection()
			}
			for _, volume := range projection.Record.Volumes {
				if volume.ID == volumeID {
					projection.Revision = entry.ModRevision
					return projection, volume, nil
				}
			}
		}
		if !page.More {
			break
		}
		if len(page.Values) == 0 {
			return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, projectionrecord.EnvironmentVolumeIdentity{}, errs.New(
				errs.KindInternal, "Environment desired-head pagination did not advance",
			)
		}
	}
	return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, projectionrecord.EnvironmentVolumeIdentity{}, errs.New(
		errs.KindVolumeNotFound, "volume was not found",
	)
}
