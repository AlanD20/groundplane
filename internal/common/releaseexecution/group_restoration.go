package releaseexecution

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// GroupRestorationAuthority binds a separate restoration work item to a real
// later member failure and an earlier successful member. It does not relabel the
// earlier successful attempt as failed or authorize ordinary image-only Rollback.
// The Controller verifies these references against the accepted Group policy
// before publishing input and before every restorative effect grant.
type GroupRestorationAuthority struct {
	GroupID          string              `json:"group_id"`
	GroupOperationID string              `json:"group_operation_id"`
	GroupInput       workexecution.Input `json:"group_input"`
	MemberOrdinal    uint32              `json:"member_ordinal"`
	FailedOrdinal    uint32              `json:"failed_ordinal"`
	FailedWorkID     string              `json:"failed_work_id"`
	Failure          workexecution.Input `json:"failure"`
	FailedAt         time.Time           `json:"failed_at"`
	SourceInput      workexecution.Input `json:"source_input"`
}

func (authority GroupRestorationAuthority) ValidateFor(attempt, source workexecution.Attempt) error {
	if authority.ValidateSource(source) != nil || attempt.Validate() != nil ||
		attempt.Input.ID != authority.SourceInput.ID || attempt.Input == authority.SourceInput ||
		attempt.Direction != workexecution.Forward ||
		attempt.FinishedAt != nil || attempt.TaskID != source.TaskID || attempt.WorkItemID == source.WorkItemID ||
		authority.FailedWorkID == attempt.WorkItemID ||
		attempt.Executor.Executor != workexecution.Agent ||
		attempt.Executor.HostID != source.Executor.HostID || attempt.AssignedAt.Before(authority.FailedAt) {
		return invalidProgress()
	}
	return nil
}

// ValidateSource validates a real completed source before a restoration attempt
// exists. Input preparation must not invent an attempt to prove this authority.
// ValidateFor additionally binds the eventual live restoration assignment.
func (authority GroupRestorationAuthority) ValidateSource(source workexecution.Attempt) error {
	if source.Validate() != nil || ids.Validate(ids.KindReleaseGroup, authority.GroupID) != nil ||
		ids.Validate(ids.KindOperation, authority.GroupOperationID) != nil ||
		authority.GroupInput.Validate() != nil || authority.Failure.Validate() != nil ||
		ids.Validate(ids.KindWorkItem, authority.FailedWorkID) != nil ||
		authority.MemberOrdinal == 0 || authority.MemberOrdinal >= authority.FailedOrdinal || authority.FailedOrdinal > 32 ||
		authority.SourceInput != source.Input || authority.SourceInput.Validate() != nil ||
		source.Direction != workexecution.Forward ||
		source.Outcome != workexecution.Succeeded || source.FinishedAt == nil || source.Result == nil ||
		source.StartedAt == nil || authority.FailedWorkID == source.WorkItemID || source.Executor.Executor != workexecution.Agent ||
		authority.FailedAt.IsZero() || authority.FailedAt.Location() != time.UTC || authority.FailedAt.Before(*source.FinishedAt) {
		return invalidProgress()
	}
	return nil
}

func CloneGroupRestoration(authority *GroupRestorationAuthority) *GroupRestorationAuthority {
	if authority == nil {
		return nil
	}
	owned := *authority
	return &owned
}

func EqualGroupRestoration(left, right *GroupRestorationAuthority) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
