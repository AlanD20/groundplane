package attachments

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	blueprintAttachTaskIntentPrefix             = "/v1/records/blueprint-attach-task-intents/"
	MaximumEnvironmentBlueprintAttachCandidates = 2
)

// BlueprintAttachTaskIntent is the non-secret, task-owned lifecycle manifest
// for every Attach introduced by one Blueprint application.
type BlueprintAttachTaskIntent struct {
	TaskID               string                 `json:"task_id"`
	EnvironmentID        string                 `json:"environment_id"`
	Status               taskjournal.TaskStatus `json:"status"`
	OwnsEnvironmentFence bool                   `json:"owns_environment_fence"`
	Candidates           []Record               `json:"candidates"`
	CreatedAt            time.Time              `json:"created_at"`
	TerminalAt           *time.Time             `json:"terminal_at,omitempty"`
}

func BlueprintAttachTaskIntentKey(taskID string) string {
	return blueprintAttachTaskIntentPrefix + taskID
}

func EncodeBlueprintAttachTaskIntent(intent BlueprintAttachTaskIntent) ([]byte, error) {
	if err := ValidateBlueprintAttachTaskIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("blueprint_attach_task_intent", intent)
}

func DecodeBlueprintAttachTaskIntent(value []byte) (BlueprintAttachTaskIntent, error) {
	intent, err := recordcodec.Decode[BlueprintAttachTaskIntent](value, "blueprint_attach_task_intent")
	if err != nil {
		return BlueprintAttachTaskIntent{}, err
	}
	if err := ValidateBlueprintAttachTaskIntent(intent); err != nil {
		return BlueprintAttachTaskIntent{}, errs.New(errs.KindInternal, "Blueprint Attach Task intent is corrupt")
	}
	return intent, nil
}

func TerminalBlueprintAttachTaskIntent(
	intent BlueprintAttachTaskIntent,
	status taskjournal.TaskStatus,
	terminalAt time.Time,
) (BlueprintAttachTaskIntent, error) {
	if intent.Status != taskjournal.TaskStatusPending || !taskjournal.IsTerminalTaskStatus(status) ||
		terminalAt.IsZero() {
		return BlueprintAttachTaskIntent{}, errs.New(errs.KindStateConflict, "Blueprint Attach intent is not pending")
	}
	terminal := intent
	terminal.Candidates = make([]Record, len(intent.Candidates))
	for index, record := range intent.Candidates {
		terminal.Candidates[index] = CloneAttachRecord(record)
	}
	terminal.Status = status
	terminalTime := terminalAt.UTC()
	terminal.TerminalAt = &terminalTime
	if err := ValidateBlueprintAttachTaskIntent(terminal); err != nil {
		return BlueprintAttachTaskIntent{}, err
	}
	return terminal, nil
}

func SameBlueprintAttachCandidateRecord(left, right Record) bool {
	return left.ID == right.ID && left.EnvironmentID == right.EnvironmentID && left.Name == right.Name &&
		left.BackingProjectID == right.BackingProjectID && left.BackingEnvironmentID == right.BackingEnvironmentID &&
		left.BackingServiceID == right.BackingServiceID && left.BackingNetworkID == right.BackingNetworkID &&
		left.ServiceID == right.ServiceID && left.CredentialAttachID == right.CredentialAttachID &&
		SameBlueprintAttachStrings(left.GrantAttachIDs, right.GrantAttachIDs) &&
		left.HookBundle == right.HookBundle && SameBlueprintAttachFactSets(left.FactSets, right.FactSets) &&
		left.Status == right.Status && left.Operation == right.Operation && left.TaskID == right.TaskID &&
		left.CreatedAt == right.CreatedAt
}

func SameBlueprintAttachStrings(left, right []string) bool {
	return (left == nil) == (right == nil) && slices.Equal(left, right)
}

func SameBlueprintAttachFactSets(left, right []FactSetMetadata) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].GrantAttachID != right[index].GrantAttachID ||
			(left[index].Facts == nil) != (right[index].Facts == nil) ||
			!slices.Equal(left[index].Facts, right[index].Facts) {
			return false
		}
	}
	return true
}

func ValidateBlueprintAttachTaskIntent(intent BlueprintAttachTaskIntent) error {
	if ids.Validate(ids.KindTask, intent.TaskID) != nil ||
		ids.Validate(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		intent.CreatedAt.IsZero() ||
		len(intent.Candidates) == 0 {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach Task intent is invalid")
	}
	if intent.Status == taskjournal.TaskStatusPending {
		if intent.TerminalAt != nil {
			return errs.New(errs.KindValidationFailed, "Pending Blueprint Attach intent has a terminal timestamp")
		}
	} else if !taskjournal.IsTerminalTaskStatus(intent.Status) || intent.TerminalAt == nil ||
		intent.TerminalAt.Before(intent.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Terminal Blueprint Attach intent is invalid")
	}
	previousID := ""
	for _, record := range intent.Candidates {
		if ValidateAttachRecord(record) != nil || record.EnvironmentID != intent.EnvironmentID ||
			record.TaskID != intent.TaskID || record.Status != core.AttachPending ||
			record.Operation != AttachOperationProvision || record.ID <= previousID {
			return errs.New(errs.KindValidationFailed, "Blueprint Attach Task candidate is invalid or unsorted")
		}
		previousID = record.ID
	}
	return nil
}

func CloneAttachRecord(record Record) Record {
	record.GrantAttachIDs = append([]string(nil), record.GrantAttachIDs...)
	record.FactSets = CloneAttachFactSets(record.FactSets)
	return record
}
