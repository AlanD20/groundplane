package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type runnerAllocationState struct {
	quota  etcdstore.Versioned[runnerallocation.RunnerTenantQuota]
	host   runnerHostSlotState
	system etcdstore.Versioned[runnerallocation.SystemPoolRegistry]
}

type runnerHostSlotState struct {
	slot      uint32
	record    runnerrecord.RunnerHostSlotRecord
	condition etcdstore.Condition
}

const (
	runnerAllocationQuotaIndex = iota
	runnerAllocationSystemIndex
	runnerAllocationHostStartIndex
)

func (repository *RunnerRepository) getRunnerAllocationState(
	ctx context.Context,
	tenantID string,
	runnerID string,
	config runnerallocation.RunnerAllocationConfig,
	slotCount uint32,
) (runnerAllocationState, error) {
	keys := make([]string, 0, runnerAllocationHostStartIndex+int(slotCount))
	keys = append(keys, runnerrecord.RunnerTenantQuotaKey(tenantID), runnerrecord.SystemPoolRegistryKey)
	for slot := uint32(0); slot < slotCount; slot++ {
		keys = append(keys, runnerrecord.RunnerHostSlotKey(slot))
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: keys,
	})
	if err != nil {
		return runnerAllocationState{}, err
	}
	if result == nil || len(result.Values) != len(keys) {
		return runnerAllocationState{}, errs.New(errs.KindInternal, "runner allocation read is incomplete")
	}
	state := runnerAllocationState{
		quota: etcdstore.Versioned[runnerallocation.RunnerTenantQuota]{
			Record: runnerallocation.RunnerTenantQuota{RunnerIDs: []string{}}, ReadRevision: result.ReadRevision,
		},
	}
	if result.Values[runnerAllocationQuotaIndex] != nil {
		state.quota.Record, err = runnerrecord.DecodeRunnerTenantQuota(result.Values[runnerAllocationQuotaIndex].Value)
		if err != nil || state.quota.Record.Validate() != nil {
			return runnerAllocationState{}, runnerrecord.CorruptRunnerTenantQuota()
		}
		state.quota.Revision = result.Values[runnerAllocationQuotaIndex].ModRevision
	}
	if result.Values[runnerAllocationSystemIndex] != nil {
		if len(result.Values[runnerAllocationSystemIndex].Value) > runnerrecord.MaximumRunnerPersistenceBytes {
			return runnerAllocationState{}, runnerrecord.CorruptSystemPoolRegistry()
		}
		state.system.Record, err = runnerrecord.DecodeSystemPoolRegistry(result.Values[runnerAllocationSystemIndex].Value)
		if err != nil || state.system.Record.Validate(config.SystemPool) != nil ||
			state.system.Record.RunnerNetworkPool != config.RunnerPool.String() {
			return runnerAllocationState{}, runnerrecord.CorruptSystemPoolRegistry()
		}
		state.system.Revision = result.Values[runnerAllocationSystemIndex].ModRevision
	} else {
		return runnerAllocationState{}, errs.New(
			errs.KindStateConflict,
			"runner network pool is not reserved at bootstrap",
		)
	}
	firstFree := int64(-1)
	owners := make(map[string]struct{}, slotCount)
	for slot := uint32(0); slot < slotCount; slot++ {
		value := result.Values[runnerAllocationHostStartIndex+int(slot)]
		if value == nil {
			if firstFree < 0 {
				firstFree = int64(slot)
			}
			continue
		}
		record, decodeErr := runnerrecord.DecodeRunnerHostSlotRecord(value.Value)
		if decodeErr != nil || record.Slot != slot {
			return runnerAllocationState{}, runnerrecord.CorruptRunnerHostSlotRecord()
		}
		if _, duplicate := owners[record.RunnerID]; duplicate {
			return runnerAllocationState{}, runnerrecord.CorruptRunnerHostSlotRecord()
		}
		owners[record.RunnerID] = struct{}{}
		if record.RunnerID == runnerID {
			return runnerAllocationState{}, stateConflict("runner", runnerID)
		}
	}
	selected := firstFree
	if selected < 0 {
		return runnerAllocationState{}, errs.New(errs.KindResourceInUse, "runner host allocation pool is exhausted")
	}
	state.host.slot = uint32(selected)
	state.host.record = runnerrecord.RunnerHostSlotRecord{Slot: uint32(selected), RunnerID: runnerID}
	state.host.condition = etcdstore.Condition{Key: runnerrecord.RunnerHostSlotKey(uint32(selected))}
	return state, nil
}

func revisionChanged(value *etcdstore.KeyValue, expected int64) bool {
	return (expected == 0 && value != nil) ||
		(expected > 0 && (value == nil || value.ModRevision != expected))
}
