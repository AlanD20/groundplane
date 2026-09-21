package releasequeries

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ReleaseRollbackSelection struct {
	Source domain.Intent
	View   ReleaseView
}

func (ledger *Reader) SelectRollback(
	ctx context.Context,
	environmentID string,
	serviceID string,
	explicitTag string,
	revision int64,
) (ReleaseRollbackSelection, error) {
	if ctx == nil || ledger == nil || ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindService, serviceID) != nil || revision <= 0 ||
		explicitTag != strings.TrimSpace(explicitTag) {
		return ReleaseRollbackSelection{}, errs.New(errs.KindValidationFailed, "rollback selection input is invalid")
	}
	projectionRead, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{releases.ReleaseProjectionKey(serviceID)}, Revision: revision,
	})
	if err != nil {
		return ReleaseRollbackSelection{}, err
	}
	if projectionRead == nil || len(projectionRead.Values) != 1 || projectionRead.Values[0] == nil {
		return ReleaseRollbackSelection{}, errs.New(
			errs.KindRollbackNoPreviousRelease,
			"service has no serving release",
		)
	}
	projection, err := releases.DecodeReleaseRecord[domain.ServiceProjection](
		projectionRead.Values[0].Value,
		"service-release-projection",
	)
	if err != nil || projection.EnvironmentID != environmentID || projection.ServiceID != serviceID {
		return ReleaseRollbackSelection{}, releases.CorruptReleaseRecord()
	}
	servingTag := ""
	if projection.ServingReleaseID != "" {
		servingIndex, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
			releases.ReleaseServiceIndexKey(environmentID, serviceID, projection.ServingReleaseID),
		}, Revision: revision})
		if err != nil {
			return ReleaseRollbackSelection{}, err
		}
		if servingIndex == nil || servingIndex.ReadRevision != revision || len(servingIndex.Values) != 1 ||
			servingIndex.Values[0] == nil {
			return ReleaseRollbackSelection{}, releases.CorruptReleaseRecord()
		}
		publicationID, err := DecodeReleaseIndex(servingIndex.Values[0].Value, serviceID)
		if err != nil {
			return ReleaseRollbackSelection{}, err
		}
		serving, err := ledger.ReadViewAt(ctx, publicationID, projection.ServingReleaseID, revision)
		if err != nil {
			return ReleaseRollbackSelection{}, err
		}
		if !rollbackIntentMatches(serving, environmentID, serviceID) {
			return ReleaseRollbackSelection{}, releases.CorruptReleaseRecord()
		}
		servingTag = serving.Intent.Tag
	}
	prefix := releases.ReleaseServiceIndexScope(environmentID, serviceID)
	start := ""
	structural := false
	materialUnavailable := false
	for {
		page, err := ledger.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: 200, Revision: revision, Descending: true,
		})
		if err != nil {
			return ReleaseRollbackSelection{}, err
		}
		if page == nil || page.ReadRevision != revision {
			return ReleaseRollbackSelection{}, releases.CorruptReleaseRecord()
		}
		for _, indexed := range page.Values {
			releaseID := strings.TrimPrefix(indexed.Key, prefix)
			publicationID, err := DecodeReleaseIndex(indexed.Value, serviceID)
			if err != nil {
				return ReleaseRollbackSelection{}, err
			}
			view, err := ledger.ReadViewAt(ctx, publicationID, releaseID, revision)
			if err != nil {
				return ReleaseRollbackSelection{}, err
			}
			if !rollbackIntentMatches(view, environmentID, serviceID) {
				return ReleaseRollbackSelection{}, releases.CorruptReleaseRecord()
			}
			if releaseID == projection.ServingReleaseID || view.Intent.Tag == servingTag ||
				explicitTag != "" && view.Intent.Tag != explicitTag || view.Checkpoint.State != domain.StateCompleted ||
				view.Terminal == nil || view.Terminal.Outcome != domain.StateCompleted ||
				view.Terminal.FinalServingReleaseID != releaseID {
				continue
			}
			structural = true
			if view.Retention == nil || view.Retention.Status != domain.RetentionAvailable {
				materialUnavailable = true
				continue
			}
			return ReleaseRollbackSelection{Source: view.Intent, View: view}, nil
		}
		if len(page.Values) == 0 || !page.More {
			break
		}
		start = page.Values[len(page.Values)-1].Key
	}
	if structural && materialUnavailable {
		return ReleaseRollbackSelection{}, errs.New(
			errs.KindRollbackSourceExpired,
			"rollback source material is expired",
		)
	}
	return ReleaseRollbackSelection{}, errs.New(
		errs.KindRollbackNoPreviousRelease,
		"no eligible previous release exists",
	)
}

func rollbackIntentMatches(view ReleaseView, environmentID, serviceID string) bool {
	return view.Intent.EnvironmentID == environmentID && view.Intent.ServiceID == serviceID
}
