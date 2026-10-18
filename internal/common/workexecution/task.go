package workexecution

import (
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

type Action string

const (
	ActionDeploy      Action = "deploy"
	ActionRollback    Action = "rollback"
	ActionBackup      Action = "backup"
	ActionBackupPrune Action = "backup_prune"
	ActionRestore     Action = "restore"
	ActionAttach      Action = "attach"
	ActionDetach      Action = "detach"
	ActionRun         Action = "run"
	ActionScript      Action = "script"
	ActionProvision   Action = "provision"
	ActionCreate      Action = "create"
	ActionUpdate      Action = "update"
	ActionRemove      Action = "remove"
	ActionStart       Action = "start"
	ActionStop        Action = "stop"
	ActionDestroy     Action = "destroy"
	ActionRotate      Action = "rotate"
	ActionFetch       Action = "fetch"
	ActionPrepare     Action = "prepare"
	ActionApply       Action = "apply"
	ActionActivate    Action = "activate"
	ActionCheck       Action = "check"
)

type Actor string

const (
	OperatorActor Actor = "operator"
	SystemActor   Actor = "system"
)

// Scope is frozen at acceptance so rename/removal cannot move Task history.
// Empty TenantID is the platform scope, including platform-owned Backing Projects.
type Scope struct {
	TenantID      string `json:"tenant_id,omitempty"`
	ProjectID     string `json:"project_id,omitempty"`
	EnvironmentID string `json:"environment_id,omitempty"`
}

// Progress is maintained in the same transaction as each item transition. It is
// a count of recorded work states, not a percentage of execution time.
type Progress struct {
	Waiting    uint32 `json:"waiting"`
	Ready      uint32 `json:"ready"`
	Assigned   uint32 `json:"assigned"`
	Running    uint32 `json:"running"`
	Attention  uint32 `json:"attention"`
	Succeeded  uint32 `json:"succeeded"`
	Failed     uint32 `json:"failed"`
	Cancelled  uint32 `json:"cancelled"`
	Superseded uint32 `json:"superseded"`
}

// Task is the operator action, independent of its number of execution attempts.
// It contains neither executable payloads nor disposable diagnostic events.
// PublicationComplete is a barrier: staged work cannot be assigned before it.
type Task struct {
	ID                  string           `json:"id"`
	Actor               Actor            `json:"actor"`
	Scope               Scope            `json:"scope"`
	Action              Action           `json:"action"`
	TargetID            string           `json:"target_id"`
	TargetName          string           `json:"target_name"`
	Intent              Input            `json:"intent"`
	CreatedAt           time.Time        `json:"created_at"`
	UpdatedAt           time.Time        `json:"updated_at"`
	Deadline            time.Time        `json:"deadline"`
	ExpectedItems       uint32           `json:"expected_items"`
	PublicationComplete bool             `json:"publication_complete"`
	Progress            Progress         `json:"progress"`
	Outcome             Outcome          `json:"outcome,omitempty"`
	FirstFailure        *Input           `json:"first_failure,omitempty"`
	FinishedAt          *time.Time       `json:"finished_at,omitempty"`
	RetainUntil         *time.Time       `json:"retain_until,omitempty"`
	CollectionStartedAt *time.Time       `json:"collection_started_at,omitempty"`
	Stop                *StopRequest     `json:"stop,omitempty"`
	Activation          *ActivationGrant `json:"activation,omitempty"`
}

// StopRequest revokes future forward grants for the whole Task in one write.
// Already-authorized effects remain owned until their operation accounts for them.
type StopRequest struct {
	Outcome     Outcome   `json:"outcome"`
	RequestedAt time.Time `json:"requested_at"`
}

func (scope Scope) Validate() error {
	if scope.TenantID != "" && ids.Validate(ids.KindTenant, scope.TenantID) != nil ||
		scope.ProjectID != "" && ids.Validate(ids.KindProject, scope.ProjectID) != nil ||
		scope.EnvironmentID != "" && (scope.ProjectID == "" || ids.Validate(ids.KindEnvironment, scope.EnvironmentID) != nil) {
		return invalid("task scope is invalid")
	}
	return nil
}

func ValidAction(action Action) bool {
	switch action {
	case ActionDeploy, ActionRollback, ActionBackup, ActionBackupPrune, ActionRestore,
		ActionAttach, ActionDetach, ActionRun, ActionScript, ActionProvision,
		ActionCreate, ActionUpdate, ActionRemove, ActionStart, ActionStop,
		ActionDestroy, ActionRotate, ActionFetch, ActionPrepare, ActionApply, ActionActivate, ActionCheck:
		return true
	default:
		return false
	}
}

func (task Task) Validate() error {
	if ids.Validate(ids.KindTask, task.ID) != nil || !ValidAction(task.Action) ||
		(task.Actor != OperatorActor && task.Actor != SystemActor) || !validResourceID(task.TargetID) ||
		len(task.TargetName) > 256 || !utf8.ValidString(task.TargetName) {
		return invalid("task identity, action or target is invalid")
	}
	if err := task.Scope.Validate(); err != nil {
		return err
	}
	if err := task.Intent.Validate(); err != nil {
		return err
	}
	if !validTime(task.CreatedAt) || !validTime(task.UpdatedAt) || !validTime(task.Deadline) ||
		task.UpdatedAt.Before(task.CreatedAt) || !task.Deadline.After(task.CreatedAt) ||
		(task.ExpectedItems == 0 && task.Action != ActionApply && task.Action != ActionRemove) || task.ExpectedItems > MaximumTaskItems ||
		task.Progress.Total() > uint64(task.ExpectedItems) ||
		task.PublicationComplete && task.Progress.Total() != uint64(task.ExpectedItems) {
		return invalid("task publication or timing is inconsistent")
	}
	if task.FirstFailure != nil {
		if err := task.FirstFailure.Validate(); err != nil {
			return err
		}
	}
	if task.Stop != nil && (task.Stop.Outcome != Cancelled && task.Stop.Outcome != Superseded ||
		!validTime(task.Stop.RequestedAt) || task.Stop.RequestedAt.Before(task.CreatedAt) ||
		task.Stop.RequestedAt.After(task.UpdatedAt)) {
		return invalid("task stop request is invalid")
	}
	if task.Activation != nil {
		if err := task.Activation.Validate(); err != nil {
			return err
		}
		if task.Stop != nil || task.Activation.GrantedAt.Before(task.CreatedAt) || task.Activation.GrantedAt.After(task.UpdatedAt) {
			return invalid("software activation conflicts with task cancellation or timing")
		}
	}
	if task.Outcome != "" && !validOutcome(task.Outcome) {
		return invalid("task outcome is invalid")
	}
	if task.FinishedAt == nil {
		if task.RetainUntil != nil {
			return invalid("unfinished task cannot enter retention")
		}
	} else if !validTime(*task.FinishedAt) || task.FinishedAt.Before(task.CreatedAt) ||
		task.FinishedAt.After(task.UpdatedAt) || task.Outcome == "" ||
		!task.PublicationComplete || task.Progress.Unsettled() != 0 || task.RetainUntil == nil ||
		!validTime(*task.RetainUntil) || !task.RetainUntil.After(*task.FinishedAt) {
		return invalid("finished task must retain a settled outcome")
	}
	if task.Outcome == Succeeded && (task.Stop != nil || !task.PublicationComplete || task.Progress.Succeeded != task.ExpectedItems) {
		return invalid("task success requires every captured work item to succeed")
	}
	if task.CollectionStartedAt != nil && (task.FinishedAt == nil || task.RetainUntil == nil ||
		!validTime(*task.CollectionStartedAt) || task.CollectionStartedAt.Before(*task.RetainUntil) ||
		task.CollectionStartedAt.After(task.UpdatedAt)) {
		return invalid("task collection requires expired, fully settled history")
	}
	return nil
}

const MaximumTaskItems uint32 = 4096

func (progress Progress) Total() uint64 {
	return progress.Unsettled() + uint64(progress.Succeeded) + uint64(progress.Failed) +
		uint64(progress.Cancelled) + uint64(progress.Superseded)
}

func (progress Progress) Unsettled() uint64 {
	return uint64(progress.Waiting) + uint64(progress.Ready) + uint64(progress.Assigned) +
		uint64(progress.Running) + uint64(progress.Attention)
}

func CloneTask(task Task) Task {
	copy := task
	if task.Activation != nil {
		activation := *task.Activation
		copy.Activation = &activation
	}
	if task.Stop != nil {
		stop := *task.Stop
		copy.Stop = &stop
	}
	if task.FirstFailure != nil {
		failure := *task.FirstFailure
		copy.FirstFailure = &failure
	}
	if task.FinishedAt != nil {
		at := *task.FinishedAt
		copy.FinishedAt = &at
	}
	if task.RetainUntil != nil {
		at := *task.RetainUntil
		copy.RetainUntil = &at
	}
	if task.CollectionStartedAt != nil {
		at := *task.CollectionStartedAt
		copy.CollectionStartedAt = &at
	}
	return copy
}
