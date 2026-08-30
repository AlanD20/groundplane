package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func prepareScriptScaleFence(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	serviceID string,
	revision int64,
) (Condition, error) {
	active, err := readActiveScriptSet(ctx, store, environmentID, revision)
	if err != nil {
		return Condition{}, err
	}
	owners, err := store.Range(ctx, RangeRequest{
		Prefix: scriptSetOwnerPrefix(environmentID, active.Record.GenerationID),
		Limit:  65, Revision: active.ReadRevision,
	})
	if err != nil {
		return Condition{}, err
	}
	if owners == nil || owners.ReadRevision != active.ReadRevision || len(owners.Values) > 64 {
		return Condition{}, errs.New(errs.KindInternal, "active Script-set owner evidence is invalid")
	}
	defer clearRangeKeyValues(owners.Values)
	keys := make([]string, len(owners.Values))
	for index, owner := range owners.Values {
		keys[index] = scriptSetScriptKey(environmentID, active.Record.GenerationID, string(owner.Value))
	}
	if len(keys) != 0 {
		primaries, readErr := getManyBatchedAtRevision(ctx, store, keys, active.ReadRevision)
		if readErr != nil {
			return Condition{}, readErr
		}
		defer clearKeyValues(primaries.Values)
		for index, primary := range primaries.Values {
			if primary == nil {
				return Condition{}, errs.New(errs.KindInternal, "active Script owner references a missing record")
			}
			record, decodeErr := decodeScriptRecord(primary.Value)
			if decodeErr != nil || record.EnvironmentID != environmentID || record.Desired.ID != string(owners.Values[index].Value) {
				return Condition{}, errs.New(errs.KindInternal, "active Script owner evidence is corrupt")
			}
			if record.ServiceID == serviceID {
				return Condition{}, errs.New(errs.KindValidationFailed, "Service replicas must remain one while targeted by a Script")
			}
		}
	}
	return Condition{Key: scriptSetActiveKey(environmentID), ModRevision: active.Revision}, nil
}
