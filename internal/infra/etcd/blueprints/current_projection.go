package blueprints

import (
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func ReadCurrentProjection(
	ctx context.Context,
	store blueprintSnapshotReader,
	environmentID string,
	revision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	return ReadProjectionAtRevision(ctx, store, environmentID, revision)
}

func ReadEffectiveProjectionRevision(
	ctx context.Context,
	store blueprintSnapshotReader,
	environmentID string,
	revisionID string,
	revision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindTask, revisionID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	return ReadEffectiveProjectionRevisionAt(ctx, store, environmentID, revisionID, revision)
}

func ReadCurrentDesiredInput(
	ctx context.Context,
	store blueprintSnapshotReader,
	environmentID string,
	revision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{}, false, err
	}
	return ReadDesiredInputAtRevision(ctx, store, environmentID, revision)
}

func ReadDesiredInputRevision(
	ctx context.Context,
	store blueprintSnapshotReader,
	environmentID string,
	revisionID string,
	revision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindTask, revisionID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{}, false, err
	}
	return ReadDesiredInputRevisionAt(ctx, store, environmentID, revisionID, revision)
}
