package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type runnerAllocationEvidence struct {
	owner  *etcdstore.KeyValue
	slug   *etcdstore.KeyValue
	quota  *etcdstore.KeyValue
	host   *etcdstore.KeyValue
	system *etcdstore.KeyValue
}

func (repository *RunnerRepository) readRunnerAllocationEvidence(
	ctx context.Context,
	record runnerrecord.RunnerRecord,
	revision int64,
) (runnerAllocationEvidence, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			runnerrecord.RunnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, record.Desired.ID),
			runnerrecord.RunnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug),
			runnerrecord.RunnerTenantQuotaKey(record.Desired.TenantID),
			runnerrecord.RunnerHostSlotKey(record.Allocation.Slot),
			runnerrecord.SystemPoolRegistryKey,
		},
		Revision: revision,
	})
	if err != nil {
		return runnerAllocationEvidence{}, err
	}
	if result == nil || len(result.Values) != 5 {
		return runnerAllocationEvidence{}, errs.New(errs.KindInternal, "runner allocation evidence is incomplete")
	}
	evidence := runnerAllocationEvidence{
		owner:  result.Values[0],
		slug:   result.Values[1],
		quota:  result.Values[2],
		host:   result.Values[3],
		system: result.Values[4],
	}
	if err := runnerAllocationEvidenceOwns(record, evidence); err != nil {
		return runnerAllocationEvidence{}, err
	}
	return evidence, nil
}

func runnerAllocationEvidenceOwns(record runnerrecord.RunnerRecord, evidence runnerAllocationEvidence) error {
	if evidence.owner == nil || string(evidence.owner.Value) != record.Desired.ID ||
		evidence.slug == nil || string(evidence.slug.Value) != record.Desired.ID ||
		evidence.quota == nil || evidence.host == nil || evidence.system == nil {
		return errs.New(errs.KindInternal, "runner allocation evidence is incomplete")
	}
	quota, err := runnerrecord.DecodeRunnerTenantQuota(evidence.quota.Value)
	if err != nil || quota.Validate() != nil {
		return runnerrecord.CorruptRunnerTenantQuota()
	}
	index := sortSearchRunnerID(quota.RunnerIDs, record.Desired.ID)
	if index >= len(quota.RunnerIDs) || quota.RunnerIDs[index] != record.Desired.ID {
		return errs.New(errs.KindInternal, "runner tenant quota lost its owner")
	}
	host, err := runnerrecord.DecodeRunnerHostSlotRecord(evidence.host.Value)
	if err != nil || host.Slot != record.Allocation.Slot || host.RunnerID != record.Desired.ID {
		return runnerrecord.CorruptRunnerHostSlotRecord()
	}
	system, err := runnerrecord.DecodeSystemPoolRegistry(evidence.system.Value)
	if err != nil ||
		system.Reservations[runnerallocation.RunnerReservationOwner(record.Desired.ID)] != record.Allocation.NetworkCIDR {
		return runnerrecord.CorruptSystemPoolRegistry()
	}
	return nil
}

func sortSearchRunnerID(values []string, id string) int {
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
