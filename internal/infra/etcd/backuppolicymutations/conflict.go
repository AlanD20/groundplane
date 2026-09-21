package backuppolicymutations

import (
	coordinationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ClassifyBackupPolicyReplacementConflict(
	values []*etcdstore.KeyValue,
	evidence []BackupPolicyReplacementCompare,
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
			return recordcodec.StateConflict("environment", comparison.ID)
		case backupPolicyCompareProject:
			if value == nil {
				return errs.New(errs.KindProjectNotFound, "project was not found")
			}
			return recordcodec.StateConflict("project", comparison.ID)
		case BackupPolicyCompareCoordination:
			if value == nil {
				return errs.New(errs.KindInternal, "environment coordination is missing")
			}
			coordination, err := coordinationrecord.Decode(value.Value)
			if err != nil || coordination.EnvironmentID != comparison.ID {
				return errs.New(errs.KindInternal, "environment coordination is corrupt")
			}
			return recordcodec.StateConflict("environment coordination", comparison.ID)
		case backupPolicyCompareOperationLock:
			return errs.New(errs.KindResourceInUse, "environment persistence operation is in progress")
		case BackupPolicyCompareSource:
			if value == nil {
				return errs.New(errs.KindBackupSourceNotFound, "backup source was not found")
			}
			return recordcodec.StateConflict("backup source", comparison.ID)
		case BackupPolicyCompareAttach:
			if value == nil {
				return errs.New(errs.KindAttachNotFound, "attach was not found")
			}
			return recordcodec.StateConflict("attach", comparison.ID)
		case backupPolicyCompareVolume:
			if value == nil {
				return errs.New(errs.KindVolumeNotFound, "volume was not found")
			}
			return recordcodec.StateConflict("volume", comparison.ID)
		case BackupPolicyCompareConnector:
			if value == nil {
				return errs.New(errs.KindConnectorNotFound, "connector was not found")
			}
			return recordcodec.StateConflict("connector", comparison.ID)
		case BackupPolicyCompareSourceEnvironmentIndex,
			BackupPolicyCompareSourceIdentityIndex,
			BackupPolicyCompareTargetOwnerIndex,
			BackupPolicyCompareConnectorOwnerIndex:
			return recordcodec.CorruptRecord()
		case backupPolicyCompareHierarchyTombstone,
			BackupPolicyCompareTargetTombstone,
			BackupPolicyCompareConnectorTombstone:
			return errs.New(errs.KindResourceInUse, "backup policy dependency deletion is in progress")
		case BackupPolicyCompareConnectorReference:
			return recordcodec.StateConflict("backup policy connector reference", comparison.ID)
		case BackupPolicyCompareKey:
			return recordcodec.StateConflict("backup key", comparison.ID)
		case BackupPolicyComparePolicy:
			return recordcodec.StateConflict("backup policy", comparison.ID)
		default:
			return errs.New(errs.KindInternal, "backup policy replacement compare kind is invalid")
		}
	}
	return errs.New(errs.KindStateConflict, "backup policy replacement state changed")
}
