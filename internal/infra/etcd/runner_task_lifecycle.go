package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

const (
	TaskResourceRunner                  = "runner"
	RunnerRegistrationTokenPresentParam = "registration_token_present"
	RunnerTenantIDParam                 = "runner_tenant_id"
	RunnerOwnerKindParam                = "runner_owner_kind"
	RunnerOwnerIDParam                  = "runner_owner_id"
	RunnerHostSlotParam                 = "runner_host_slot"
	RunnerNetworkCIDRParam              = "runner_network_cidr"
	runnerRemovalIntentPrefix           = "/v1/runtime/runner-removal-intents/"
)

type runnerRemovalTaskEvidence struct {
	tenantID    string
	ownerKind   runnerrecord.RunnerOwnerKind
	ownerID     string
	hostSlot    uint32
	networkCIDR string
}

// RunnerRemovalTaskParams encodes the canonical durable input for one Runner
// removal Task. Publication and validation must share this representation.
func RunnerRemovalTaskParams(record runnerrecord.RunnerRecord) map[string]string {
	return map[string]string{
		TaskResourceKindParam:  TaskResourceRunner,
		RunnerTenantIDParam:    record.Desired.TenantID,
		RunnerOwnerKindParam:   string(record.Desired.OwnerKind),
		RunnerOwnerIDParam:     record.Desired.OwnerID,
		RunnerHostSlotParam:    runnerHostSlotSegment(record.Allocation.Slot),
		RunnerNetworkCIDRParam: record.Allocation.NetworkCIDR,
	}
}

func decodeRunnerRemovalTaskEvidence(task TaskRecord) (runnerRemovalTaskEvidence, error) {
	if task.Executor != taskjournal.TaskExecutorController || task.Type != taskjournal.TaskRemove ||
		ids.Validate(ids.KindRunner, task.Target) != nil || len(task.Params) != 6 ||
		task.Params[TaskResourceKindParam] != TaskResourceRunner ||
		ids.Validate(ids.KindTenant, task.Params[RunnerTenantIDParam]) != nil {
		return runnerRemovalTaskEvidence{}, errs.New(errs.KindInternal, "runner removal task has invalid durable input")
	}
	ownerKind := runnerrecord.RunnerOwnerKind(task.Params[RunnerOwnerKindParam])
	ownerID := task.Params[RunnerOwnerIDParam]
	if (ownerKind == runnerrecord.RunnerOwnerTenant &&
		(ids.Validate(ids.KindTenant, ownerID) != nil || ownerID != task.Params[RunnerTenantIDParam])) ||
		(ownerKind == runnerrecord.RunnerOwnerProject && ids.Validate(ids.KindProject, ownerID) != nil) ||
		(ownerKind != runnerrecord.RunnerOwnerTenant && ownerKind != runnerrecord.RunnerOwnerProject) {
		return runnerRemovalTaskEvidence{}, errs.New(errs.KindInternal, "runner removal task has invalid durable input")
	}
	expectedOwner, err := runnerTaskOwner(runnerrecord.RunnerDesiredRecord{
		ID: task.Target, OwnerKind: ownerKind, OwnerID: ownerID, TenantID: task.Params[RunnerTenantIDParam],
	})
	if err != nil || task.Owner != expectedOwner {
		return runnerRemovalTaskEvidence{}, errs.New(errs.KindInternal, "runner removal task has invalid owner")
	}
	hostSlot, err := parseRunnerHostSlotSegment(task.Params[RunnerHostSlotParam])
	if err != nil {
		return runnerRemovalTaskEvidence{}, errs.New(errs.KindInternal, "runner removal task has invalid durable input")
	}
	if _, err := runnerrecord.RunnerAllocationPrefix(task.Params[RunnerNetworkCIDRParam]); err != nil {
		return runnerRemovalTaskEvidence{}, errs.New(errs.KindInternal, "runner removal task has invalid durable input")
	}
	return runnerRemovalTaskEvidence{
		tenantID: task.Params[RunnerTenantIDParam], ownerKind: ownerKind, ownerID: ownerID,
		hostSlot: hostSlot, networkCIDR: task.Params[RunnerNetworkCIDRParam],
	}, nil
}

func (evidence runnerRemovalTaskEvidence) matchesRecord(record runnerrecord.RunnerRecord) bool {
	return evidence.tenantID == record.Desired.TenantID &&
		evidence.ownerKind == record.Desired.OwnerKind && evidence.ownerID == record.Desired.OwnerID &&
		evidence.hostSlot == record.Allocation.Slot && evidence.networkCIDR == record.Allocation.NetworkCIDR
}

func (evidence runnerRemovalTaskEvidence) matchesIntent(intent RunnerRemovalIntent) bool {
	return evidence.tenantID == intent.TenantID && evidence.ownerKind == intent.OwnerKind &&
		evidence.ownerID == intent.OwnerID && evidence.hostSlot == intent.Allocation.Slot &&
		evidence.networkCIDR == intent.Allocation.NetworkCIDR
}

type RunnerRemovalIntent struct {
	RunnerID   string                                      `json:"runner_id"`
	TaskID     string                                      `json:"task_id"`
	OwnerKind  runnerrecord.RunnerOwnerKind                `json:"owner_kind"`
	OwnerID    string                                      `json:"owner_id"`
	TenantID   string                                      `json:"tenant_id"`
	Allocation runnerallocation.RunnerHostAllocationRecord `json:"allocation"`
	CreatedAt  time.Time                                   `json:"created_at"`
}

func runnerRemovalIntentKey(runnerID string) string {
	return runnerRemovalIntentPrefix + runnerID
}

func validateRunnerRemovalIntent(intent RunnerRemovalIntent) error {
	desired := runnerrecord.RunnerDesiredRecord{
		ID: intent.RunnerID, OwnerKind: intent.OwnerKind, OwnerID: intent.OwnerID, TenantID: intent.TenantID,
	}
	if runnerrecord.ValidateRunnerOwnership(desired) != nil || ids.Validate(ids.KindTask, intent.TaskID) != nil ||
		intent.Allocation.Validate() != nil || !recordcodec.IsCanonicalUTC(intent.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "runner removal intent is invalid")
	}
	return nil
}

func encodeRunnerRemovalIntent(intent RunnerRemovalIntent) ([]byte, error) {
	if err := validateRunnerRemovalIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("runner_removal_intent", intent)
}

func decodeRunnerRemovalIntent(value []byte) (RunnerRemovalIntent, error) {
	if len(value) > runnerrecord.MaximumRunnerPersistenceBytes {
		return RunnerRemovalIntent{}, errs.New(errs.KindInternal, "runner removal intent is corrupt")
	}
	intent, err := recordcodec.Decode[RunnerRemovalIntent](value, "runner_removal_intent")
	if err != nil || validateRunnerRemovalIntent(intent) != nil {
		return RunnerRemovalIntent{}, errs.New(errs.KindInternal, "runner removal intent is corrupt")
	}
	return intent, nil
}

func validateRunnerDeletionTombstone(record deletionrecord.DeletionTombstoneRecord) error {
	if record.TargetKind != deletionrecord.DeletionTargetRunner || ids.Validate(ids.KindRunner, record.TargetID) != nil ||
		record.TargetRevision <= 0 || ids.Validate(ids.KindTask, record.TaskID) != nil ||
		record.Phase != deletionrecord.DeletionPhaseFinalizing || record.Checkpoint != (deletionrecord.DeletionCheckpoint{}) ||
		!recordcodec.IsCanonicalUTC(record.CreatedAt) || !recordcodec.IsCanonicalUTC(record.UpdatedAt) ||
		record.UpdatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "runner deletion tombstone is invalid")
	}
	return nil
}

func encodeRunnerDeletionTombstone(record deletionrecord.DeletionTombstoneRecord) ([]byte, error) {
	if err := validateRunnerDeletionTombstone(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("deletion-tombstone", record)
}

func decodeRunnerDeletionTombstone(value []byte) (deletionrecord.DeletionTombstoneRecord, error) {
	if len(value) > runnerrecord.MaximumRunnerPersistenceBytes {
		return deletionrecord.DeletionTombstoneRecord{}, errs.New(errs.KindInternal, "runner deletion tombstone is corrupt")
	}
	record, err := recordcodec.Decode[deletionrecord.DeletionTombstoneRecord](value, "deletion-tombstone")
	if err != nil || validateRunnerDeletionTombstone(record) != nil {
		return deletionrecord.DeletionTombstoneRecord{}, errs.New(errs.KindInternal, "runner deletion tombstone is corrupt")
	}
	return record, nil
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
	if task.Executor != taskjournal.TaskExecutorController || task.Params[TaskResourceKindParam] != TaskResourceRunner {
		return false, nil
	}
	switch task.Type {
	case TaskCreate:
		return taskOwnsRunnerCreation(task)
	case TaskRemove:
		_, err := decodeRunnerRemovalTaskEvidence(task)
		return err == nil, err
	default:
		return false, errs.New(errs.KindInternal, "runner task has invalid durable input")
	}
}

func taskOwnsRunnerCreation(task TaskRecord) (bool, error) {
	if task.Executor != taskjournal.TaskExecutorController || task.Params[TaskResourceKindParam] != TaskResourceRunner {
		return false, nil
	}
	if task.Type != taskjournal.TaskCreate || ids.Validate(ids.KindRunner, task.Target) != nil ||
		len(task.Params) != 2 || task.Params[RunnerRegistrationTokenPresentParam] != "true" {
		return false, errs.New(errs.KindInternal, "runner creation task has invalid durable input")
	}
	return true, nil
}

func runnerIntentMatchesRecord(intent RunnerRemovalIntent, record runnerrecord.RunnerRecord, taskID string) bool {
	return intent.RunnerID == record.Desired.ID && intent.TaskID == taskID &&
		intent.OwnerKind == record.Desired.OwnerKind && intent.OwnerID == record.Desired.OwnerID &&
		intent.TenantID == record.Desired.TenantID && intent.Allocation == record.Allocation
}

func runnerRetryableTerminal(status taskjournal.TaskStatus) bool {
	return status == taskjournal.TaskStatusFailed || status == taskjournal.TaskStatusAborted || status == taskjournal.TaskStatusTimedOut
}

func runnerTerminal(status taskjournal.TaskStatus) bool {
	return status == taskjournal.TaskStatusCompleted || runnerRetryableTerminal(status)
}

func clearRunnerTaskChange(change runnerTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
