package taskjournal

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type PruneIntent struct {
	TaskID                                 string `json:"task_id"`
	TaskRevision                           int64  `json:"task_revision"`
	BackupCheckpointCursorsComplete        bool   `json:"backup_checkpoint_cursors_complete"`
	BackupCheckpointDeduplicationsComplete bool   `json:"backup_checkpoint_deduplications_complete"`
	TaskPrimaryDeleted                     bool   `json:"task_primary_deleted"`
	BackupTerminalReceiptRevision          int64  `json:"backup_terminal_receipt_revision,omitempty"`
	AttachPlanID                           string `json:"attach_plan_id,omitempty"`
	RemainingEvents                        uint32 `json:"remaining_events"`
	RemainingDeduplications                uint32 `json:"remaining_deduplications"`
}

func EncodePruneIntent(intent PruneIntent) ([]byte, error) {
	if err := ValidatePruneIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("task_prune_intent", intent)
}

func DecodePruneIntent(value []byte) (PruneIntent, error) {
	intent, err := recordcodec.Decode[PruneIntent](value, "task_prune_intent")
	if err != nil {
		return PruneIntent{}, err
	}
	if err := ValidatePruneIntent(intent); err != nil {
		return PruneIntent{}, CorruptPruneIntent()
	}
	return intent, nil
}

func ValidatePruneIntent(intent PruneIntent) error {
	if ids.Validate(ids.KindTask, intent.TaskID) != nil || intent.TaskRevision <= 0 ||
		(intent.BackupCheckpointDeduplicationsComplete &&
			!intent.BackupCheckpointCursorsComplete) ||
		(intent.TaskPrimaryDeleted && !intent.BackupCheckpointDeduplicationsComplete) ||
		intent.BackupTerminalReceiptRevision < 0 ||
		intent.RemainingEvents > MaximumTaskEvents ||
		intent.RemainingDeduplications > MaximumTaskEvents {
		return errs.New(errs.KindValidationFailed, "task prune intent is invalid")
	}
	if intent.AttachPlanID != "" && ids.Validate(ids.KindPlan, intent.AttachPlanID) != nil {
		return errs.New(errs.KindValidationFailed, "task prune Attach plan is invalid")
	}
	return nil
}
