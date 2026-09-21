package runners

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RunnerAllocationEvidence struct {
	Owner  *etcdstore.KeyValue
	Slug   *etcdstore.KeyValue
	Quota  *etcdstore.KeyValue
	Host   *etcdstore.KeyValue
	System *etcdstore.KeyValue
}

func (repository *Reader) ReadRunnerAllocationEvidence(
	ctx context.Context,
	record RunnerRecord,
	revision int64,
) (RunnerAllocationEvidence, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			RunnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, record.Desired.ID),
			RunnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug),
			RunnerTenantQuotaKey(record.Desired.TenantID),
			RunnerHostSlotKey(record.Allocation.Slot),
			SystemPoolRegistryKey,
		},
		Revision: revision,
	})
	if err != nil {
		return RunnerAllocationEvidence{}, err
	}
	if result == nil || len(result.Values) != 5 {
		return RunnerAllocationEvidence{}, errs.New(errs.KindInternal, "runner allocation evidence is incomplete")
	}
	evidence := RunnerAllocationEvidence{
		Owner:  result.Values[0],
		Slug:   result.Values[1],
		Quota:  result.Values[2],
		Host:   result.Values[3],
		System: result.Values[4],
	}
	if err := RunnerAllocationEvidenceOwns(record, evidence); err != nil {
		return RunnerAllocationEvidence{}, err
	}
	return evidence, nil
}

func RunnerAllocationEvidenceOwns(record RunnerRecord, evidence RunnerAllocationEvidence) error {
	if evidence.Owner == nil || string(evidence.Owner.Value) != record.Desired.ID ||
		evidence.Slug == nil || string(evidence.Slug.Value) != record.Desired.ID ||
		evidence.Quota == nil || evidence.Host == nil || evidence.System == nil {
		return errs.New(errs.KindInternal, "runner allocation evidence is incomplete")
	}
	quota, err := DecodeRunnerTenantQuota(evidence.Quota.Value)
	if err != nil || quota.Validate() != nil {
		return CorruptRunnerTenantQuota()
	}
	index := SortSearchRunnerID(quota.RunnerIDs, record.Desired.ID)
	if index >= len(quota.RunnerIDs) || quota.RunnerIDs[index] != record.Desired.ID {
		return errs.New(errs.KindInternal, "runner tenant quota lost its owner")
	}
	host, err := DecodeRunnerHostSlotRecord(evidence.Host.Value)
	if err != nil || host.Slot != record.Allocation.Slot || host.RunnerID != record.Desired.ID {
		return CorruptRunnerHostSlotRecord()
	}
	system, err := DecodeSystemPoolRegistry(evidence.System.Value)
	if err != nil ||
		system.Reservations[runnerallocation.RunnerReservationOwner(record.Desired.ID)] != record.Allocation.NetworkCIDR {
		return CorruptSystemPoolRegistry()
	}
	return nil
}

func SortSearchRunnerID(values []string, id string) int {
	left, right := 0, len(values)
	for left < right {
		middle := int(uint(left+right) >> 1)
		if values[middle] < id {
			left = middle + 1
		} else {
			right = middle
		}
	}
	return left
}
