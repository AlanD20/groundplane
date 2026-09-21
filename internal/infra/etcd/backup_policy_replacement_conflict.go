package etcd

import (
	coordinationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func classifyBackupPolicyReplacementConflict(
	values []*etcdstore.KeyValue,
	evidence []backupPolicyReplacementCompare,
) error {
	if len(values) != len(evidence) {
		return errs.New(errs.KindInternal, "backup policy replacement compare evidence is incomplete")
	}
	for index, comparison := range evidence {
		value := values[index]
		if (comparison.ExpectedRevision == 0 && value == nil) ||
			(comparison.ExpectedRevision > 0 && value != nil && value.ModRevision == comparison.ExpectedRevision) {
			continue
		}
		switch comparison.Kind {
		case backupPolicyCompareEnvironment:
			if value == nil {
				return errs.New(errs.KindEnvironmentNotFound, "environment was not found")
			}
			return stateConflict("environment", comparison.ID)
		case backupPolicyCompareProject:
			if value == nil {
				return errs.New(errs.KindProjectNotFound, "project was not found")
			}
			return stateConflict("project", comparison.ID)
		case backupPolicyCompareCoordination:
			if value == nil {
				return errs.New(errs.KindInternal, "environment coordination is missing")
			}
			coordination, err := coordinationrecord.Decode(value.Value)
			if err != nil || coordination.EnvironmentID != comparison.ID {
				return errs.New(errs.KindInternal, "environment coordination is corrupt")
			}
			return stateConflict("environment coordination", comparison.ID)
		case backupPolicyCompareOperationLock:
			return errs.New(errs.KindResourceInUse, "environment persistence operation is in progress")
		case backupPolicyCompareSource:
			if value == nil {
				return errs.New(errs.KindBackupSourceNotFound, "backup source was not found")
			}
			return stateConflict("backup source", comparison.ID)
		case backupPolicyCompareAttach:
			if value == nil {
				return errs.New(errs.KindAttachNotFound, "attach was not found")
			}
			return stateConflict("attach", comparison.ID)
		case backupPolicyCompareVolume:
			if value == nil {
				return errs.New(errs.KindVolumeNotFound, "volume was not found")
			}
			return stateConflict("volume", comparison.ID)
		case backupPolicyCompareConnector:
			if value == nil {
				return errs.New(errs.KindConnectorNotFound, "connector was not found")
			}
			return stateConflict("connector", comparison.ID)
		case backupPolicyCompareSourceEnvironmentIndex,
			backupPolicyCompareSourceIdentityIndex,
			backupPolicyCompareTargetOwnerIndex,
			backupPolicyCompareConnectorOwnerIndex:
			return recordcodec.CorruptRecord()
		case backupPolicyCompareHierarchyTombstone,
			backupPolicyCompareTargetTombstone,
			backupPolicyCompareConnectorTombstone:
			return errs.New(errs.KindResourceInUse, "backup policy dependency deletion is in progress")
		case backupPolicyCompareConnectorReference:
			return stateConflict("backup policy connector reference", comparison.ID)
		case backupPolicyCompareKey:
			return stateConflict("backup key", comparison.ID)
		case backupPolicyComparePolicy:
			return stateConflict("backup policy", comparison.ID)
		default:
			return errs.New(errs.KindInternal, "backup policy replacement compare kind is invalid")
		}
	}
	return errs.New(errs.KindStateConflict, "backup policy replacement state changed")
}
