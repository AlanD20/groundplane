package etcd

import (
	"context"
	"crypto/sha256"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// volumeRemovalEvidenceConditions binds the private immutable seal to this
// exact removal and desired baseline. The publisher fences all three records:
// cleanup cannot invalidate the staged set between this read and publication.
func (repository *HierarchyRepository) volumeRemovalEvidenceConditions(
	ctx context.Context, runtime removal.Runtime, sourceRevisionID string, readRevision int64,
) ([]etcdstore.Condition, error) {
	keys := []string{
		removal.EvidenceManifestKey(runtime.OperationID),
		removal.EvidenceCursorKey(runtime.OperationID),
		removal.EvidenceSealKey(runtime.OperationID),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return nil, err
	}
	if read == nil {
		return nil, volumeRemovalEvidenceConflict()
	}
	defer clearKeyValues(read.Values)
	if read.ReadRevision != readRevision || len(read.Values) != len(keys) {
		return nil, volumeRemovalEvidenceConflict()
	}
	conditions := make([]etcdstore.Condition, len(keys))
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > readRevision {
			return nil, volumeRemovalEvidenceConflict()
		}
		conditions[index] = etcdstore.Condition{Key: keys[index], ModRevision: value.ModRevision}
	}
	manifest, err := removal.DecodeEvidenceManifest(read.Values[0].Value)
	if err != nil {
		return nil, err
	}
	if manifest.OperationID != runtime.OperationID || manifest.EnvironmentID != runtime.EnvironmentID ||
		manifest.VolumeID != runtime.VolumeID || manifest.Key != runtime.Key || manifest.SourceRevisionID != sourceRevisionID ||
		manifest.DesiredRevisionID != runtime.DesiredRevisionID || manifest.ImpactSHA256 != runtime.ImpactSHA256 ||
		sha256.Sum256(read.Values[0].Value) != runtime.EvidenceManifestSHA256 {
		return nil, volumeRemovalEvidenceConflict()
	}
	cursor, err := removal.DecodeEvidenceCursor(read.Values[1].Value, manifest)
	if err != nil {
		return nil, err
	}
	seal, err := removal.DecodeEvidenceSeal(read.Values[2].Value, manifest)
	if err != nil {
		return nil, err
	}
	if cursor.CompletedRows != manifest.TotalRows || seal.ManifestRevision != read.Values[0].ModRevision ||
		seal.CursorRevision != read.Values[1].ModRevision || seal.VerifiedRevision >= read.Values[2].ModRevision {
		return nil, volumeRemovalEvidenceConflict()
	}
	return conditions, nil
}

func volumeRemovalEvidenceConflict() error {
	return errs.New(errs.KindStateConflict, "Volume removal sealed evidence changed or is incomplete")
}
