package environmentqueries

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
)

// FindEnvironmentVolumeIdentity selects the declared owner before touching
// runtime. Unfinished rendering in another Environment cannot block this lookup.
func (repository *ProjectionReader) FindEnvironmentVolumeIdentity(ctx context.Context, volumeID string) (
	etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities], projectionrecord.EnvironmentVolumeIdentity, error,
) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, projectionrecord.EnvironmentVolumeIdentity{}, err
	}
	if ids.Validate(ids.KindVolume, volumeID) != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, projectionrecord.EnvironmentVolumeIdentity{}, errs.New(
			errs.KindValidationFailed,
			"Volume id is invalid",
		)
	}
	start := ""
	revision := int64(0)
	for {
		page, err := repository.store.Range(
			ctx,
			etcdstore.RangeRequest{
				Prefix:         EnvironmentDesiredHeadScanPrefix,
				StartExclusive: start,
				Limit:          128,
				Revision:       revision,
			},
		)
		if err != nil {
			return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, projectionrecord.EnvironmentVolumeIdentity{}, err
		}
		if page == nil || page.ReadRevision <= 0 || page.More && len(page.Values) == 0 {
			return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, projectionrecord.EnvironmentVolumeIdentity{}, errs.New(
				errs.KindInternal,
				"Environment desired-head scan did not advance",
			)
		}
		revision = page.ReadRevision
		for _, entry := range page.Values {
			start = entry.Key
			if !strings.HasSuffix(entry.Key, "/current") {
				continue
			}
			environmentID := strings.TrimSuffix(
				strings.TrimPrefix(entry.Key, EnvironmentDesiredHeadScanPrefix),
				"/current",
			)
			owned, found, err := blueprints.ReadCurrentOwnedIdentities(ctx, repository.store, environmentID, revision)
			if err != nil {
				return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, projectionrecord.EnvironmentVolumeIdentity{}, err
			}
			if !found {
				continue
			}
			for _, volume := range owned.Record.Volumes {
				if volume.ID == volumeID {
					return owned, projectionrecord.EnvironmentVolumeIdentity{
						ID:   volume.ID,
						Key:  volume.Name,
						Slug: volume.Slug,
					}, nil
				}
			}
		}
		if !page.More {
			break
		}
	}
	return etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]{}, projectionrecord.EnvironmentVolumeIdentity{}, errs.New(
		errs.KindVolumeNotFound,
		"Volume was not found",
	)
}

// FindEnvironmentVolume retains the sealed-runtime requirement for destructive
// operations and dependency inspection, after selecting the exact declared owner.
func (repository *ProjectionReader) FindEnvironmentVolume(ctx context.Context, volumeID string) (
	etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], projectionrecord.EnvironmentVolumeIdentity, error,
) {
	owned, identity, err := repository.FindEnvironmentVolumeIdentity(ctx, volumeID)
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, projectionrecord.EnvironmentVolumeIdentity{}, err
	}
	projection, found, err := blueprints.ReadEffectiveProjectionRevisionAt(
		ctx,
		repository.store,
		owned.Record.EnvironmentID,
		owned.Record.RevisionID,
		owned.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, projectionrecord.EnvironmentVolumeIdentity{}, err
	}
	if !found {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, projectionrecord.EnvironmentVolumeIdentity{}, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	projection.Revision = owned.Revision
	return projection, identity, nil
}
