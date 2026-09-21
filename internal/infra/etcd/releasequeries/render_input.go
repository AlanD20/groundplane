package releasequeries

import (
	"context"
	"encoding/json"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// GetReleaseRenderInputAt returns the immutable render input for one Release
// from the caller's fixed MVCC view. Script snapshots use this instead of
// rebuilding a service definition from mutable desired state.
func (ledger *Reader) GetReleaseRenderInputAt(
	ctx context.Context,
	releaseID string,
	revision int64,
) (etcdstore.Versioned[releaserender.ReleaseRenderInput], error) {
	if ctx == nil || ledger == nil || ledger.store == nil ||
		ids.Validate(ids.KindDeployment, releaseID) != nil || revision <= 0 {
		return etcdstore.Versioned[releaserender.ReleaseRenderInput]{}, errs.New(
			errs.KindValidationFailed,
			"Script Release render input request is invalid",
		)
	}
	read, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{releases.ReleaseRenderInputStagingKey("", releaseID)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[releaserender.ReleaseRenderInput]{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return etcdstore.Versioned[releaserender.ReleaseRenderInput]{}, errs.New(
			errs.KindReleaseNotFound,
			"successful Release render input was not found",
		)
	}
	raw, err := releases.DecodeReleaseRecord[json.RawMessage](read.Values[0].Value, "release-render-input")
	if err != nil {
		return etcdstore.Versioned[releaserender.ReleaseRenderInput]{}, releases.CorruptReleaseRecord()
	}
	input, err := releaserender.DecodeReleaseRenderInput(raw)
	if err != nil || input.ReleaseID != releaseID {
		return etcdstore.Versioned[releaserender.ReleaseRenderInput]{}, releases.CorruptReleaseRecord()
	}
	return etcdstore.Versioned[releaserender.ReleaseRenderInput]{
		Record: input, Revision: read.Values[0].ModRevision, ReadRevision: revision,
	}, nil
}
