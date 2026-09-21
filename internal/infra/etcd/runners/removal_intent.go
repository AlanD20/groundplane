package runners

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
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

// RunnerRemovalTaskParams encodes the canonical durable input for one Runner
// removal Task. Publication and validation must share this representation.
func RunnerRemovalTaskParams(record RunnerRecord) map[string]string {
	return map[string]string{
		taskjournal.TaskResourceKindParam: TaskResourceRunner,
		RunnerTenantIDParam:               record.Desired.TenantID,
		RunnerOwnerKindParam:              string(record.Desired.OwnerKind),
		RunnerOwnerIDParam:                record.Desired.OwnerID,
		RunnerHostSlotParam:               RunnerHostSlotSegment(record.Allocation.Slot),
		RunnerNetworkCIDRParam:            record.Allocation.NetworkCIDR,
	}
}

type RunnerRemovalIntent struct {
	RunnerID   string                                      `json:"runner_id"`
	TaskID     string                                      `json:"task_id"`
	OwnerKind  RunnerOwnerKind                             `json:"owner_kind"`
	OwnerID    string                                      `json:"owner_id"`
	TenantID   string                                      `json:"tenant_id"`
	Allocation runnerallocation.RunnerHostAllocationRecord `json:"allocation"`
	CreatedAt  time.Time                                   `json:"created_at"`
}

func RunnerRemovalIntentKey(runnerID string) string {
	return runnerRemovalIntentPrefix + runnerID
}

func validateRunnerRemovalIntent(intent RunnerRemovalIntent) error {
	desired := RunnerDesiredRecord{
		ID: intent.RunnerID, OwnerKind: intent.OwnerKind, OwnerID: intent.OwnerID, TenantID: intent.TenantID,
	}
	if ValidateRunnerOwnership(desired) != nil || ids.Validate(ids.KindTask, intent.TaskID) != nil ||
		intent.Allocation.Validate() != nil || !recordcodec.IsCanonicalUTC(intent.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "runner removal intent is invalid")
	}
	return nil
}

func EncodeRunnerRemovalIntent(intent RunnerRemovalIntent) ([]byte, error) {
	if err := validateRunnerRemovalIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("runner_removal_intent", intent)
}

func DecodeRunnerRemovalIntent(value []byte) (RunnerRemovalIntent, error) {
	if len(value) > MaximumRunnerPersistenceBytes {
		return RunnerRemovalIntent{}, errs.New(errs.KindInternal, "runner removal intent is corrupt")
	}
	intent, err := recordcodec.Decode[RunnerRemovalIntent](value, "runner_removal_intent")
	if err != nil || validateRunnerRemovalIntent(intent) != nil {
		return RunnerRemovalIntent{}, errs.New(errs.KindInternal, "runner removal intent is corrupt")
	}
	return intent, nil
}

func ValidateRunnerDeletionTombstone(record deletionrecord.DeletionTombstoneRecord) error {
	if record.TargetKind != deletionrecord.DeletionTargetRunner || ids.Validate(ids.KindRunner, record.TargetID) != nil ||
		record.TargetRevision <= 0 ||
		ids.Validate(ids.KindTask, record.TaskID) != nil ||
		record.Phase != deletionrecord.DeletionPhaseFinalizing ||
		record.Checkpoint != (deletionrecord.DeletionCheckpoint{}) ||
		!recordcodec.IsCanonicalUTC(record.CreatedAt) ||
		!recordcodec.IsCanonicalUTC(record.UpdatedAt) ||
		record.UpdatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "runner deletion tombstone is invalid")
	}
	return nil
}

func EncodeRunnerDeletionTombstone(record deletionrecord.DeletionTombstoneRecord) ([]byte, error) {
	if err := ValidateRunnerDeletionTombstone(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("deletion-tombstone", record)
}

func DecodeRunnerDeletionTombstone(value []byte) (deletionrecord.DeletionTombstoneRecord, error) {
	if len(value) > MaximumRunnerPersistenceBytes {
		return deletionrecord.DeletionTombstoneRecord{}, errs.New(
			errs.KindInternal,
			"runner deletion tombstone is corrupt",
		)
	}
	record, err := recordcodec.Decode[deletionrecord.DeletionTombstoneRecord](value, "deletion-tombstone")
	if err != nil || ValidateRunnerDeletionTombstone(record) != nil {
		return deletionrecord.DeletionTombstoneRecord{}, errs.New(
			errs.KindInternal,
			"runner deletion tombstone is corrupt",
		)
	}
	return record, nil
}

func RunnerIntentMatchesRecord(intent RunnerRemovalIntent, record RunnerRecord, taskID string) bool {
	return intent.RunnerID == record.Desired.ID && intent.TaskID == taskID &&
		intent.OwnerKind == record.Desired.OwnerKind && intent.OwnerID == record.Desired.OwnerID &&
		intent.TenantID == record.Desired.TenantID && intent.Allocation == record.Allocation
}
