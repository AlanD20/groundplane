package backupruntime

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
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

func (repository *Reader) ResolveVolumeRemovalImpactAtRevision(
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
		return BackupVolumeRemovalImpact{}, CorruptBackupRuntimeRecord()
	}
	defer etcdstore.ClearValues(policyRead.Values)
	selected := make(map[string]struct{})
	impact := BackupVolumeRemovalImpact{}
	if policyRead.Values[0] != nil {
		policy, decodeErr := backuppolicy.DecodeBackupPolicyRecord(policyRead.Values[0].Value)
		if decodeErr != nil || policy.EnvironmentID != environmentID {
			return BackupVolumeRemovalImpact{}, CorruptBackupRuntimeRecord()
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
			return BackupVolumeRemovalImpact{}, CorruptBackupRuntimeRecord()
		}
		sourceRevision := sourceRead.Values[0].ModRevision
		etcdstore.ClearValues(sourceRead.Values)
		impact.Sources = append(impact.Sources, BackupVolumeSourceImpact{
			SourceID: sourceID, SourceRevision: sourceRevision, Selected: isSelected,
			RecoveryPointCount: points, HistoricalDigest: digest,
		})
	}
	impact.PolicyDisables = impact.PolicyRevision > 0 && len(selected) > 0 && remainingSelected == 0
	return impact, nil
}

func (repository *Reader) backupVolumeSourceIDsAtRevision(
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
			return nil, CorruptBackupRuntimeRecord()
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
				return nil, CorruptBackupRuntimeRecord()
			}
			source, decodeErr := backuppolicy.DecodeBackupSourceRecord(read.Values[0].Value)
			etcdstore.ClearValues(read.Values)
			if decodeErr != nil || source.ID != sourceID || source.EnvironmentID != environmentID {
				return nil, CorruptBackupRuntimeRecord()
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

func (repository *Reader) backupVolumeHistoricalImpactAtRevision(
	ctx context.Context,
	sourceID string,
	volumeID string,
	revision int64,
) (int64, string, error) {
	prefix := BackupRecoveryPointSourcePrefix + sourceID + "/"
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
			return 0, "", CorruptBackupRuntimeRecord()
		}
		for _, index := range page.Values {
			pointID := string(index.Value)
			clear(index.Value)
			if ids.Validate(ids.KindRecoveryPoint, pointID) != nil {
				return 0, "", CorruptBackupRuntimeRecord()
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
			Keys: []string{BackupRecoveryPointKey(pointID)}, Revision: revision,
		})
		if err != nil {
			return 0, "", err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
			return 0, "", CorruptBackupRuntimeRecord()
		}
		point, decodeErr := DecodeBackupRecoveryPointRecord(read.Values[0].Value)
		if decodeErr != nil || point.ID != pointID || point.SourceID != sourceID || point.TargetID != volumeID {
			etcdstore.ClearValues(read.Values)
			return 0, "", CorruptBackupRuntimeRecord()
		}
		writeBackupImpactField(digest, []byte(pointID))
		writeBackupImpactField(digest, read.Values[0].Value)
		etcdstore.ClearValues(read.Values)
	}
	return int64(len(pointIDs)), hex.EncodeToString(digest.Sum(nil)), nil
}

func writeBackupImpactField(digest hash.Hash, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	digest.Write(size[:])
	digest.Write(value)
}
