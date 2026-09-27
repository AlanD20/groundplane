package hierarchydeletionfinalization

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Blueprint Apply persists these defaults even without configured backups. Retire
// only inert metadata in the same CAS as its Environment; live authority still
// requires the separate child-cleanup proof.
func prepareIdleEnvironmentMetadata(environmentID string, policy, coordination *keyvalue.KeyValue) (Effects, error) {
	policyKey, coordinationKey := backuppolicy.BackupPolicyKey(
		environmentID,
	), environmentcoordination.Key(
		environmentID,
	)
	if policy != nil {
		record, err := backuppolicy.DecodeBackupPolicyRecord(policy.Value)
		if err != nil || policy.Key != policyKey || record.EnvironmentID != environmentID {
			return Effects{}, errs.New(errs.KindInternal, "environment deletion Backup policy metadata is corrupt")
		}
		if record.Enabled || record.Frequency != "" || record.Keep != 0 || record.Encryption != "" ||
			record.ConnectorID != "" || len(record.SourceIDs) != 0 {
			return Effects{}, errs.New(errs.KindStateConflict, "environment retained configured Backup policy")
		}
	}
	if coordination != nil {
		record, err := environmentcoordination.Decode(coordination.Value)
		if err != nil || coordination.Key != coordinationKey || record.EnvironmentID != environmentID {
			return Effects{}, errs.New(errs.KindInternal, "environment deletion coordination metadata is corrupt")
		}
		if record.CurrentBackupScheduleState != nil {
			return Effects{}, errs.New(errs.KindStateConflict, "environment retained Backup scheduling authority")
		}
	}
	return Effects{
		conditions: []keyvalue.Condition{
			{Key: policyKey, ModRevision: keyvalue.RevisionOf(policy)},
			{Key: coordinationKey, ModRevision: keyvalue.RevisionOf(coordination)},
		},
		mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationDelete, Key: policyKey},
			{Type: keyvalue.MutationDelete, Key: coordinationKey},
		},
	}, nil
}
