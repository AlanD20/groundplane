package environmentqueries

import (
	"context"
	"encoding/hex"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BackupVolumeProjectionEvidence struct {
	Environment      etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	Projection       etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	ProjectionRoot   int64
	DependencyDigest string
	Volume           projectionrecord.EnvironmentVolumeIdentity
}

func (repository *ProjectionReader) ResolveEnvironmentVolumeAtRevision(
	ctx context.Context,
	environmentID string,
	volumeID string,
	revisionID string,
	fixedRevision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], projectionrecord.EnvironmentVolumeIdentity, error) {
	evidence, err := LoadBackupVolumeProjectionEvidence(
		ctx, repository.store, environmentID, volumeID, fixedRevision,
	)
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, projectionrecord.EnvironmentVolumeIdentity{}, err
	}
	if evidence.Projection.Record.RevisionID != revisionID {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, projectionrecord.EnvironmentVolumeIdentity{}, errs.New(
			errs.KindStateConflict, "Volume desired revision changed",
		)
	}
	return evidence.Projection, evidence.Volume, nil
}

func LoadBackupVolumeProjectionEvidence(
	ctx context.Context,
	store projectionStore,
	environmentID string,
	volumeID string,
	fixedRevision int64,
) (BackupVolumeProjectionEvidence, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindVolume, volumeID) != nil || fixedRevision < 0 {
		return BackupVolumeProjectionEvidence{}, errs.New(
			errs.KindValidationFailed, "backup Volume projection lookup is invalid",
		)
	}
	headRead, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{hierarchyrecord.EnvironmentKey(environmentID), blueprints.EnvironmentBlueprintHeadKey(environmentID)},
		Revision: fixedRevision,
	})
	if err != nil {
		return BackupVolumeProjectionEvidence{}, err
	}
	if headRead == nil || len(headRead.Values) != 2 || headRead.ReadRevision <= 0 ||
		headRead.Values[0] == nil || headRead.Values[1] == nil {
		return BackupVolumeProjectionEvidence{}, errs.New(errs.KindVolumeNotFound, "volume was not found")
	}
	defer etcdstore.ClearValues(headRead.Values)
	if fixedRevision > 0 && headRead.ReadRevision != fixedRevision {
		return BackupVolumeProjectionEvidence{}, recordcodec.CorruptRecord()
	}
	environment, err := hierarchyrecord.DecodeEnvironment(headRead.Values[0].Value)
	revisionID, headErr := idempotencyrecord.DecodeTaskReference(headRead.Values[1].Value)
	if err != nil || headErr != nil || environment.ID != environmentID ||
		ids.Validate(ids.KindTask, revisionID) != nil {
		return BackupVolumeProjectionEvidence{}, recordcodec.CorruptRecord()
	}
	fixedRevision = headRead.ReadRevision
	rootKey := blueprints.EnvironmentBlueprintRootKey(environmentID, revisionID)
	rootRead, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{rootKey}, Revision: fixedRevision})
	if err != nil {
		return BackupVolumeProjectionEvidence{}, err
	}
	if rootRead == nil || rootRead.ReadRevision != fixedRevision || len(rootRead.Values) != 1 ||
		rootRead.Values[0] == nil {
		return BackupVolumeProjectionEvidence{}, recordcodec.CorruptRecord()
	}
	defer etcdstore.ClearValues(rootRead.Values)
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(rootRead.Values[0].Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != revisionID {
		return BackupVolumeProjectionEvidence{}, recordcodec.CorruptRecord()
	}
	if store == nil {
		return BackupVolumeProjectionEvidence{}, errs.New(errs.KindInternal, "hierarchy store is required")
	}
	hierarchy := NewProjectionReader(store)
	projection, found, err := hierarchy.GetEnvironmentComposeProjectionRevision(ctx, environmentID, revisionID)
	if err != nil {
		return BackupVolumeProjectionEvidence{}, err
	}
	if !found || projection.Revision != rootRead.Values[0].ModRevision ||
		projection.Record.RenderGeneration != seal.RenderGeneration {
		return BackupVolumeProjectionEvidence{}, recordcodec.CorruptRecord()
	}
	projectionDigest, _, err := blueprints.EnvironmentBlueprintProjectionEvidence(projection.Record)
	if err != nil || projectionDigest != seal.ProjectionSHA256 {
		return BackupVolumeProjectionEvidence{}, recordcodec.CorruptRecord()
	}
	dependencyDigest, err := blueprints.EnvironmentBlueprintDependencyDigest(projection.Record)
	if err != nil || dependencyDigest != seal.DependencyDigest {
		return BackupVolumeProjectionEvidence{}, recordcodec.CorruptRecord()
	}
	identity := projectionrecord.EnvironmentVolumeIdentity{}
	for _, candidate := range projection.Record.Volumes {
		if candidate.ID == volumeID {
			identity = candidate
			break
		}
	}
	if identity.ID == "" {
		return BackupVolumeProjectionEvidence{}, errs.New(errs.KindVolumeNotFound, "volume was not found")
	}
	projection.Revision = headRead.Values[1].ModRevision
	projection.ReadRevision = fixedRevision
	return BackupVolumeProjectionEvidence{
		Environment: etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{
			Record: environment, Revision: headRead.Values[0].ModRevision, ReadRevision: fixedRevision,
		},
		Projection: projection, ProjectionRoot: rootRead.Values[0].ModRevision,
		DependencyDigest: hex.EncodeToString(dependencyDigest[:]), Volume: identity,
	}, nil
}
