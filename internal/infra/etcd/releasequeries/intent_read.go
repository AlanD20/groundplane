package releasequeries

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (ledger *Reader) GetIntent(
	ctx context.Context,
	publicationID, releaseID string,
) (etcdstore.Versioned[domain.Intent], error) {
	if ctx == nil || ledger == nil || releases.ValidatePublicationID(publicationID) != nil ||
		ids.Validate(ids.KindDeployment, releaseID) != nil {
		return etcdstore.Versioned[domain.Intent]{}, errs.New(
			errs.KindValidationFailed,
			"release read identity is invalid",
		)
	}
	result, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		releases.ReleaseIntentStagingKey(publicationID, releaseID), releases.ReleasePublicationKey(publicationID),
	}})
	if err != nil {
		return etcdstore.Versioned[domain.Intent]{}, err
	}
	if result == nil || len(result.Values) != 2 || result.Values[1] == nil {
		return etcdstore.Versioned[domain.Intent]{}, errs.New(errs.KindReleaseNotFound, "release was not found")
	}
	if result.Values[0] == nil {
		return etcdstore.Versioned[domain.Intent]{}, releases.CorruptReleaseRecord()
	}
	intent, err := releases.DecodeReleaseRecord[domain.Intent](result.Values[0].Value, "release-intent")
	if err != nil || domain.ValidateIntent(intent) != nil || intent.ID != releaseID {
		return etcdstore.Versioned[domain.Intent]{}, releases.CorruptReleaseRecord()
	}
	return etcdstore.Versioned[domain.Intent]{
		Record:       intent,
		Revision:     result.Values[0].ModRevision,
		ReadRevision: result.ReadRevision,
	}, nil
}
