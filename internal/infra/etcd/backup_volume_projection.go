package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"hash"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BackupVolumeSourceImpact struct {
	SourceID           string
	SourceRevision     int64
	Selected           bool
	RecoveryPointCount int64
	HistoricalDigest   string
}

type BackupVolumeRemovalImpact struct {
	PolicyRevision int64
	PolicyDisables bool
	Sources        []BackupVolumeSourceImpact
}

type backupVolumeProjectionEvidence struct {
	Environment      etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	Projection       etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	ProjectionRoot   int64
	DependencyDigest string
	Volume           projectionrecord.EnvironmentVolumeIdentity
}

func (repository *HierarchyRepository) ResolveEnvironmentVolumeAtRevision(
	ctx context.Context,
	environmentID string,
	volumeID string,
	revisionID string,
	fixedRevision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], projectionrecord.EnvironmentVolumeIdentity, error) {
	evidence, err := loadBackupVolumeProjectionEvidence(
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

func (repository *HierarchyRepository) ResolveVolumeRemovalImpactAtRevision(
	ctx context.Context,
	environmentID string,
	volumeID string,
	revision int64,
	policyNow time.Time,
) (BackupVolumeRemovalImpact, error) {
	backup, err := newBackupRuntimeRepository(repository.store)
	if err != nil {
		return BackupVolumeRemovalImpact{}, err
	}
	return backup.ResolveVolumeRemovalImpactAtRevision(ctx, environmentID, volumeID, revision, policyNow)
}

func (repository *BackupRuntimeRepository) ResolveVolumeRemovalImpactAtRevision(
	ctx context.Context,
	environmentID string,
	volumeID string,
	revision int64,
	_ time.Time,
) (BackupVolumeRemovalImpact, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindVolume, volumeID) != nil || revision <= 0 {
		return BackupVolumeRemovalImpact{}, errs.New(
			errs.KindValidationFailed, "Volume Backup impact lookup is invalid",
		)
	}
	policyRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{backuppolicy.BackupPolicyKey(environmentID)}, Revision: revision,
	})
	if err != nil {
		return BackupVolumeRemovalImpact{}, err
	}
	if policyRead == nil || policyRead.ReadRevision != revision || len(policyRead.Values) != 1 {
		return BackupVolumeRemovalImpact{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer clearKeyValues(policyRead.Values)
	selected := make(map[string]struct{})
	impact := BackupVolumeRemovalImpact{}
	if policyRead.Values[0] != nil {
		policy, decodeErr := backuppolicy.DecodeBackupPolicyRecord(policyRead.Values[0].Value)
		if decodeErr != nil || policy.EnvironmentID != environmentID {
			return BackupVolumeRemovalImpact{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		impact.PolicyRevision = policyRead.Values[0].ModRevision
		for _, sourceID := range policy.SourceIDs {
			selected[sourceID] = struct{}{}
		}
	}
	sourceIDs, err := repository.backupVolumeSourceIDsAtRevision(ctx, environmentID, volumeID, revision)
	if err != nil {
		return BackupVolumeRemovalImpact{}, err
	}
	remainingSelected := len(selected)
	for _, sourceID := range sourceIDs {
		_, isSelected := selected[sourceID]
		if isSelected {
			remainingSelected--
		}
		points, digest, pointErr := repository.backupVolumeHistoricalImpactAtRevision(
			ctx, sourceID, volumeID, revision,
		)
		if pointErr != nil {
			return BackupVolumeRemovalImpact{}, pointErr
		}
		sourceRead, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{backuppolicy.BackupSourceKey(sourceID)}, Revision: revision,
		})
		if readErr != nil {
			return BackupVolumeRemovalImpact{}, readErr
		}
		if sourceRead == nil || sourceRead.ReadRevision != revision || len(sourceRead.Values) != 1 ||
			sourceRead.Values[0] == nil {
			return BackupVolumeRemovalImpact{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		sourceRevision := sourceRead.Values[0].ModRevision
		clearKeyValues(sourceRead.Values)
		impact.Sources = append(impact.Sources, BackupVolumeSourceImpact{
			SourceID: sourceID, SourceRevision: sourceRevision, Selected: isSelected,
			RecoveryPointCount: points, HistoricalDigest: digest,
		})
	}
	impact.PolicyDisables = impact.PolicyRevision > 0 && len(selected) > 0 && remainingSelected == 0
	return impact, nil
}

func (repository *BackupRuntimeRepository) backupVolumeSourceIDsAtRevision(
	ctx context.Context,
	environmentID string,
	volumeID string,
	revision int64,
) ([]string, error) {
	prefix := backuppolicy.BackupSourceEnvironmentPrefix(environmentID)
	start := ""
	result := make([]string, 0)
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: etcdstore.MaximumOperations, Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if page == nil || page.ReadRevision != revision || (page.More && len(page.Values) == 0) {
			return nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		for _, index := range page.Values {
			sourceID := string(index.Value)
			start = index.Key
			read, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
				Keys: []string{backuppolicy.BackupSourceKey(sourceID)}, Revision: revision,
			})
			clear(index.Value)
			if readErr != nil {
				return nil, readErr
			}
			if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
				return nil, backupruntime.CorruptBackupRuntimeRecord()
			}
			source, decodeErr := backuppolicy.DecodeBackupSourceRecord(read.Values[0].Value)
			clearKeyValues(read.Values)
			if decodeErr != nil || source.ID != sourceID || source.EnvironmentID != environmentID {
				return nil, backupruntime.CorruptBackupRuntimeRecord()
			}
			if source.Kind == "volume" && source.TargetID == volumeID {
				result = append(result, sourceID)
			}
		}
		if !page.More {
			break
		}
	}
	sort.Strings(result)
	return result, nil
}

func (repository *BackupRuntimeRepository) backupVolumeHistoricalImpactAtRevision(
	ctx context.Context,
	sourceID string,
	volumeID string,
	revision int64,
) (int64, string, error) {
	prefix := backupruntime.BackupRecoveryPointSourcePrefix + sourceID + "/"
	start := ""
	pointIDs := make([]string, 0)
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: etcdstore.MaximumOperations, Revision: revision,
		})
		if err != nil {
			return 0, "", err
		}
		if page == nil || page.ReadRevision != revision || (page.More && len(page.Values) == 0) {
			return 0, "", backupruntime.CorruptBackupRuntimeRecord()
		}
		for _, index := range page.Values {
			pointID := string(index.Value)
			clear(index.Value)
			if ids.Validate(ids.KindRecoveryPoint, pointID) != nil {
				return 0, "", backupruntime.CorruptBackupRuntimeRecord()
			}
			pointIDs = append(pointIDs, pointID)
			start = index.Key
		}
		if !page.More {
			break
		}
	}
	sort.Strings(pointIDs)
	digest := sha256.New()
	digest.Write([]byte("groundplane.volume.backup-history.v1"))
	for _, pointID := range pointIDs {
		read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{backupruntime.BackupRecoveryPointKey(pointID)}, Revision: revision,
		})
		if err != nil {
			return 0, "", err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
			return 0, "", backupruntime.CorruptBackupRuntimeRecord()
		}
		point, decodeErr := backupruntime.DecodeBackupRecoveryPointRecord(read.Values[0].Value)
		if decodeErr != nil || point.ID != pointID || point.SourceID != sourceID || point.TargetID != volumeID {
			clearKeyValues(read.Values)
			return 0, "", backupruntime.CorruptBackupRuntimeRecord()
		}
		writeBackupImpactField(digest, []byte(pointID))
		writeBackupImpactField(digest, read.Values[0].Value)
		clearKeyValues(read.Values)
	}
	return int64(len(pointIDs)), hex.EncodeToString(digest.Sum(nil)), nil
}

func writeBackupImpactField(digest hash.Hash, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	digest.Write(size[:])
	digest.Write(value)
}

func loadBackupVolumeProjectionEvidence(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	volumeID string,
	fixedRevision int64,
) (backupVolumeProjectionEvidence, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindVolume, volumeID) != nil || fixedRevision < 0 {
		return backupVolumeProjectionEvidence{}, errs.New(
			errs.KindValidationFailed, "backup Volume projection lookup is invalid",
		)
	}
	headRead, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{hierarchyrecord.EnvironmentKey(environmentID), blueprints.EnvironmentBlueprintHeadKey(environmentID)},
		Revision: fixedRevision,
	})
	if err != nil {
		return backupVolumeProjectionEvidence{}, err
	}
	if headRead == nil || len(headRead.Values) != 2 || headRead.ReadRevision <= 0 ||
		headRead.Values[0] == nil || headRead.Values[1] == nil {
		return backupVolumeProjectionEvidence{}, errs.New(errs.KindVolumeNotFound, "volume was not found")
	}
	defer clearKeyValues(headRead.Values)
	if fixedRevision > 0 && headRead.ReadRevision != fixedRevision {
		return backupVolumeProjectionEvidence{}, recordcodec.CorruptRecord()
	}
	environment, err := hierarchyrecord.DecodeEnvironment(headRead.Values[0].Value)
	revisionID, headErr := idempotencyrecord.DecodeTaskReference(headRead.Values[1].Value)
	if err != nil || headErr != nil || environment.ID != environmentID ||
		ids.Validate(ids.KindTask, revisionID) != nil {
		return backupVolumeProjectionEvidence{}, recordcodec.CorruptRecord()
	}
	fixedRevision = headRead.ReadRevision
	rootKey := blueprints.EnvironmentBlueprintRootKey(environmentID, revisionID)
	rootRead, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{rootKey}, Revision: fixedRevision})
	if err != nil {
		return backupVolumeProjectionEvidence{}, err
	}
	if rootRead == nil || rootRead.ReadRevision != fixedRevision || len(rootRead.Values) != 1 ||
		rootRead.Values[0] == nil {
		return backupVolumeProjectionEvidence{}, recordcodec.CorruptRecord()
	}
	defer clearKeyValues(rootRead.Values)
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(rootRead.Values[0].Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != revisionID {
		return backupVolumeProjectionEvidence{}, recordcodec.CorruptRecord()
	}
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		return backupVolumeProjectionEvidence{}, err
	}
	projection, found, err := hierarchy.GetEnvironmentComposeProjectionRevision(ctx, environmentID, revisionID)
	if err != nil {
		return backupVolumeProjectionEvidence{}, err
	}
	if !found || projection.Revision != rootRead.Values[0].ModRevision ||
		projection.Record.RenderGeneration != seal.RenderGeneration {
		return backupVolumeProjectionEvidence{}, recordcodec.CorruptRecord()
	}
	projectionDigest, _, err := blueprints.EnvironmentBlueprintProjectionEvidence(projection.Record)
	if err != nil || projectionDigest != seal.ProjectionSHA256 {
		return backupVolumeProjectionEvidence{}, recordcodec.CorruptRecord()
	}
	dependencyDigest, err := blueprints.EnvironmentBlueprintDependencyDigest(projection.Record)
	if err != nil || dependencyDigest != seal.DependencyDigest {
		return backupVolumeProjectionEvidence{}, recordcodec.CorruptRecord()
	}
	identity := projectionrecord.EnvironmentVolumeIdentity{}
	for _, candidate := range projection.Record.Volumes {
		if candidate.ID == volumeID {
			identity = candidate
			break
		}
	}
	if identity.ID == "" {
		return backupVolumeProjectionEvidence{}, errs.New(errs.KindVolumeNotFound, "volume was not found")
	}
	projection.Revision = headRead.Values[1].ModRevision
	projection.ReadRevision = fixedRevision
	return backupVolumeProjectionEvidence{
		Environment: etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{
			Record: environment, Revision: headRead.Values[0].ModRevision, ReadRevision: fixedRevision,
		},
		Projection: projection, ProjectionRoot: rootRead.Values[0].ModRevision,
		DependencyDigest: hex.EncodeToString(dependencyDigest[:]), Volume: identity,
	}, nil
}
