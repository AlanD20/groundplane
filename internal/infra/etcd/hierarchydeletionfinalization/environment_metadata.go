package hierarchydeletionfinalization

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Empty reservations and Blueprint defaults are metadata, not live children.
// Retire them only with exact revision comparisons in the parent deletion CAS.
func readIdleEnvironmentMetadata(
	ctx context.Context,
	store finalizationStore,
	environmentID string,
	revision int64,
) (Effects, error) {
	policyKey, coordinationKey := backuppolicy.BackupPolicyKey(
		environmentID,
	), environmentcoordination.Key(
		environmentID,
	)
	poolKey := networkreservations.ZonePoolRegistryKey(environmentID)
	stored, err := store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{policyKey, coordinationKey, poolKey}, Revision: revision,
	})
	if err != nil {
		return Effects{}, err
	}
	if stored == nil || stored.ReadRevision != revision || len(stored.Values) != 3 {
		if stored != nil {
			keyvalue.ClearValues(stored.Values)
		}
		return Effects{}, errs.New(errs.KindInternal, "environment deletion metadata evidence is incomplete")
	}
	defer keyvalue.ClearValues(stored.Values)
	policy, coordination, pool := stored.Values[0], stored.Values[1], stored.Values[2]
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
	if pool != nil {
		record, err := recordcodec.Decode[networkreservations.ZonePoolRegistry](pool.Value, "zone_pool_registry")
		if err != nil || pool.Key != poolKey || networkreservations.ValidateZonePoolRegistry(record) != nil {
			return Effects{}, errs.New(errs.KindInternal, "environment deletion Zone registry is corrupt")
		}
		if len(record.Reservations) != 0 {
			return Effects{}, errs.New(errs.KindStateConflict, "environment retained Zone reservations")
		}
	}
	return Effects{
		conditions: []keyvalue.Condition{
			{Key: policyKey, ModRevision: keyvalue.RevisionOf(policy)},
			{Key: coordinationKey, ModRevision: keyvalue.RevisionOf(coordination)},
			{Key: poolKey, ModRevision: keyvalue.RevisionOf(pool)},
		},
		mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationDelete, Key: policyKey},
			{Type: keyvalue.MutationDelete, Key: coordinationKey},
			{Type: keyvalue.MutationDelete, Key: poolKey},
		},
	}, nil
}
