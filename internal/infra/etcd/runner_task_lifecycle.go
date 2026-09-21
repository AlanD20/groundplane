package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type runnerRemovalTaskEvidence struct {
	tenantID    string
	ownerKind   runnerrecord.RunnerOwnerKind
	ownerID     string
	hostSlot    uint32
	networkCIDR string
}

func decodeRunnerRemovalTaskEvidence(task TaskRecord) (runnerRemovalTaskEvidence, error) {
	if task.Executor != taskjournal.TaskExecutorController || task.Type != taskjournal.TaskRemove ||
		ids.Validate(ids.KindRunner, task.Target) != nil || len(task.Params) != 6 ||
		task.Params[taskjournal.TaskResourceKindParam] != runnerrecord.TaskResourceRunner ||
		ids.Validate(ids.KindTenant, task.Params[runnerrecord.RunnerTenantIDParam]) != nil {
		return runnerRemovalTaskEvidence{}, errs.New(errs.KindInternal, "runner removal task has invalid durable input")
	}
	ownerKind := runnerrecord.RunnerOwnerKind(task.Params[runnerrecord.RunnerOwnerKindParam])
	ownerID := task.Params[runnerrecord.RunnerOwnerIDParam]
	if (ownerKind == runnerrecord.RunnerOwnerTenant &&
		(ids.Validate(ids.KindTenant, ownerID) != nil || ownerID != task.Params[runnerrecord.RunnerTenantIDParam])) ||
		(ownerKind == runnerrecord.RunnerOwnerProject && ids.Validate(ids.KindProject, ownerID) != nil) ||
		(ownerKind != runnerrecord.RunnerOwnerTenant && ownerKind != runnerrecord.RunnerOwnerProject) {
		return runnerRemovalTaskEvidence{}, errs.New(errs.KindInternal, "runner removal task has invalid durable input")
	}
	expectedOwner, err := runnerTaskOwner(runnerrecord.RunnerDesiredRecord{
		ID: task.Target, OwnerKind: ownerKind, OwnerID: ownerID, TenantID: task.Params[runnerrecord.RunnerTenantIDParam],
	})
	if err != nil || task.Owner != expectedOwner {
		return runnerRemovalTaskEvidence{}, errs.New(errs.KindInternal, "runner removal task has invalid owner")
	}
	hostSlot, err := runnerrecord.ParseRunnerHostSlotSegment(task.Params[runnerrecord.RunnerHostSlotParam])
	if err != nil {
		return runnerRemovalTaskEvidence{}, errs.New(errs.KindInternal, "runner removal task has invalid durable input")
	}
	if _, err := runnerrecord.RunnerAllocationPrefix(task.Params[runnerrecord.RunnerNetworkCIDRParam]); err != nil {
		return runnerRemovalTaskEvidence{}, errs.New(errs.KindInternal, "runner removal task has invalid durable input")
	}
	return runnerRemovalTaskEvidence{
		tenantID: task.Params[runnerrecord.RunnerTenantIDParam], ownerKind: ownerKind, ownerID: ownerID,
		hostSlot: hostSlot, networkCIDR: task.Params[runnerrecord.RunnerNetworkCIDRParam],
	}, nil
}

func (evidence runnerRemovalTaskEvidence) matchesRecord(record runnerrecord.RunnerRecord) bool {
	return evidence.tenantID == record.Desired.TenantID &&
		evidence.ownerKind == record.Desired.OwnerKind && evidence.ownerID == record.Desired.OwnerID &&
		evidence.hostSlot == record.Allocation.Slot && evidence.networkCIDR == record.Allocation.NetworkCIDR
}

func (evidence runnerRemovalTaskEvidence) matchesIntent(intent runnerrecord.RunnerRemovalIntent) bool {
	return evidence.tenantID == intent.TenantID && evidence.ownerKind == intent.OwnerKind &&
		evidence.ownerID == intent.OwnerID && evidence.hostSlot == intent.Allocation.Slot &&
		evidence.networkCIDR == intent.Allocation.NetworkCIDR
}

func runnerStringMapsEqual(left map[string]string, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func taskOwnsRunner(task TaskRecord) (bool, error) {
	if task.Executor != taskjournal.TaskExecutorController ||
		task.Params[taskjournal.TaskResourceKindParam] != runnerrecord.TaskResourceRunner {
		return false, nil
	}
	switch task.Type {
	case taskjournal.TaskCreate:
		return taskOwnsRunnerCreation(task)
	case taskjournal.TaskRemove:
		_, err := decodeRunnerRemovalTaskEvidence(task)
		return err == nil, err
	default:
		return false, errs.New(errs.KindInternal, "runner task has invalid durable input")
	}
}

func taskOwnsRunnerCreation(task TaskRecord) (bool, error) {
	if task.Executor != taskjournal.TaskExecutorController ||
		task.Params[taskjournal.TaskResourceKindParam] != runnerrecord.TaskResourceRunner {
		return false, nil
	}
	if task.Type != taskjournal.TaskCreate || ids.Validate(ids.KindRunner, task.Target) != nil ||
		len(task.Params) != 2 || task.Params[runnerrecord.RunnerRegistrationTokenPresentParam] != "true" {
		return false, errs.New(errs.KindInternal, "runner creation task has invalid durable input")
	}
	return true, nil
}

func runnerRetryableTerminal(status taskjournal.TaskStatus) bool {
	return status == taskjournal.TaskStatusFailed || status == taskjournal.TaskStatusAborted ||
		status == taskjournal.TaskStatusTimedOut
}

func runnerTerminal(status taskjournal.TaskStatus) bool {
	return status == taskjournal.TaskStatusCompleted || runnerRetryableTerminal(status)
}

func clearRunnerTaskChange(change runnerTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
