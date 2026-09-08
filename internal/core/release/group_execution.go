package release

import (
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type GroupMember struct {
	Ordinal   uint32 `json:"ordinal"`
	ServiceID string `json:"service_id"`
	ReleaseID string `json:"release_id"`
}

type GroupManifest struct {
	OperationID              string        `json:"operation_id"`
	ReleaseGroupID           string        `json:"release_group_id"`
	EnvironmentID            string        `json:"environment_id"`
	FailurePolicy            OnFailure     `json:"failure_policy"`
	Members                  []GroupMember `json:"members"`
	ConfiguredTimeoutSeconds int64         `json:"configured_timeout_seconds"`
	ComputedBudgetSeconds    int64         `json:"computed_budget_seconds"`
}

type MemberOutcome string

const (
	MemberPending          MemberOutcome = "pending"
	MemberServing          MemberOutcome = "serving"
	MemberCompleted        MemberOutcome = "completed"
	MemberFailed           MemberOutcome = "failed"
	MemberCompensated      MemberOutcome = "compensated"
	MemberRecoveryRequired MemberOutcome = "recovery_required"
)

type MemberResult struct {
	Ordinal               uint32        `json:"ordinal"`
	ServiceID             string        `json:"service_id"`
	ReleaseID             string        `json:"release_id"`
	Outcome               MemberOutcome `json:"outcome"`
	FailureCode           string        `json:"failure_code,omitempty"`
	FailureDetail         string        `json:"failure_detail,omitempty"`
	ServingReleaseID      string        `json:"serving_release_id,omitempty"`
	CompensationReleaseID string        `json:"compensation_release_id,omitempty"`
}

type GroupProgress struct {
	OperationID             string         `json:"operation_id"`
	AttemptID               string         `json:"attempt_id"`
	NextMemberOrdinal       uint32         `json:"next_member_ordinal"`
	Results                 []MemberResult `json:"results"`
	Compensating            bool           `json:"compensating"`
	NextCompensationOrdinal uint32         `json:"next_compensation_ordinal,omitempty"`
	UpdatedAt               time.Time      `json:"updated_at"`
}

type GroupExecutor struct {
	manifest GroupManifest
}

func NewGroupExecutor(manifest GroupManifest) (*GroupExecutor, error) {
	if err := ValidateGroupManifest(manifest); err != nil {
		return nil, err
	}
	manifest.Members = slices.Clone(manifest.Members)
	return &GroupExecutor{manifest: manifest}, nil
}

func ValidateGroupManifest(value GroupManifest) error {
	if ids.Validate(ids.KindOperation, value.OperationID) != nil ||
		ids.Validate(ids.KindReleaseGroup, value.ReleaseGroupID) != nil ||
		ids.Validate(ids.KindEnvironment, value.EnvironmentID) != nil ||
		len(value.Members) < MinimumGroupMembers ||
		len(value.Members) > MaximumGroupMembers ||
		value.ConfiguredTimeoutSeconds <= 0 ||
		value.ComputedBudgetSeconds <= 0 ||
		value.ConfiguredTimeoutSeconds < value.ComputedBudgetSeconds {
		return invalid("release group execution manifest is invalid")
	}
	if value.FailurePolicy != OnFailureSwitchBack && value.FailurePolicy != OnFailureLeaveActive {
		return invalid("release group failure policy is invalid")
	}
	services := make(map[string]struct{}, len(value.Members))
	releases := make(map[string]struct{}, len(value.Members))
	for index, member := range value.Members {
		if member.Ordinal != uint32(index+1) || ids.Validate(ids.KindService, member.ServiceID) != nil ||
			ids.Validate(ids.KindDeployment, member.ReleaseID) != nil {
			return invalid("release group member identity or order is invalid")
		}
		if _, exists := services[member.ServiceID]; exists {
			return invalid("release group service is duplicated")
		}
		if _, exists := releases[member.ReleaseID]; exists {
			return invalid("release group candidate is duplicated")
		}
		services[member.ServiceID] = struct{}{}
		releases[member.ReleaseID] = struct{}{}
	}
	return nil
}

func (executor *GroupExecutor) Begin(attemptID string, now time.Time) (GroupProgress, error) {
	if executor == nil || ids.Validate(ids.KindTask, attemptID) != nil || now.IsZero() || now.Location() != time.UTC {
		return GroupProgress{}, invalid("release group attempt is invalid")
	}
	return GroupProgress{
		OperationID:       executor.manifest.OperationID,
		AttemptID:         attemptID,
		NextMemberOrdinal: 1,
		UpdatedAt:         now,
	}, nil
}

func (executor *GroupExecutor) RecordServing(
	progress GroupProgress,
	result MemberResult,
	now time.Time,
) (GroupProgress, error) {
	if err := executor.validateProgress(progress); err != nil {
		return GroupProgress{}, err
	}
	if progress.Compensating || result.Ordinal != progress.NextMemberOrdinal || result.Outcome != MemberServing ||
		int(
			result.Ordinal,
		) > len(
			executor.manifest.Members,
		) || !sameMember(executor.manifest.Members[result.Ordinal-1], result) {
		return GroupProgress{}, errs.New(errs.KindStateConflict, "release group member checkpoint is out of order")
	}
	progress.Results = append(slices.Clone(progress.Results), result)
	progress.NextMemberOrdinal++
	progress.UpdatedAt = now
	return progress, nil
}

func (executor *GroupExecutor) RecordFailure(
	progress GroupProgress,
	result MemberResult,
	now time.Time,
) (GroupProgress, error) {
	if err := executor.validateProgress(progress); err != nil {
		return GroupProgress{}, err
	}
	if progress.Compensating || result.Ordinal != progress.NextMemberOrdinal || result.Outcome != MemberFailed ||
		result.FailureCode == "" || !sameMember(executor.manifest.Members[result.Ordinal-1], result) {
		return GroupProgress{}, errs.New(errs.KindStateConflict, "release group failure checkpoint is invalid")
	}
	progress.Results = append(slices.Clone(progress.Results), result)
	progress.NextMemberOrdinal = 0
	if executor.manifest.FailurePolicy == OnFailureSwitchBack && result.Ordinal > 1 {
		progress.Compensating = true
		progress.NextCompensationOrdinal = result.Ordinal - 1
	}
	progress.UpdatedAt = now
	return progress, nil
}

func (executor *GroupExecutor) RecordCompensation(
	progress GroupProgress,
	result MemberResult,
	now time.Time,
) (GroupProgress, error) {
	if err := executor.validateProgress(progress); err != nil {
		return GroupProgress{}, err
	}
	ordinal := progress.NextCompensationOrdinal
	if !progress.Compensating || ordinal == 0 || result.Ordinal != ordinal ||
		(result.Outcome != MemberCompensated && result.Outcome != MemberRecoveryRequired) ||
		!sameMember(executor.manifest.Members[ordinal-1], result) {
		return GroupProgress{}, errs.New(
			errs.KindStateConflict,
			"release group compensation checkpoint is out of order",
		)
	}
	results := slices.Clone(progress.Results)
	for index := range results {
		if results[index].Ordinal == ordinal {
			results[index] = result
			progress.Results = results
			if result.Outcome == MemberRecoveryRequired || ordinal == 1 {
				progress.Compensating = false
				progress.NextCompensationOrdinal = 0
			} else {
				progress.NextCompensationOrdinal--
			}
			progress.UpdatedAt = now
			return progress, nil
		}
	}
	return GroupProgress{}, errs.New(errs.KindInternal, "release group compensation target is absent")
}

func (executor *GroupExecutor) Complete(progress GroupProgress, now time.Time) (GroupProgress, error) {
	if err := executor.validateProgress(progress); err != nil {
		return GroupProgress{}, err
	}
	if progress.Compensating || progress.NextMemberOrdinal != uint32(len(executor.manifest.Members)+1) ||
		len(progress.Results) != len(executor.manifest.Members) {
		return GroupProgress{}, errs.New(errs.KindStateConflict, "release group operation is not complete")
	}
	for index := range progress.Results {
		if progress.Results[index].Outcome != MemberServing {
			return GroupProgress{}, errs.New(errs.KindStateConflict, "release group member has not reached serving")
		}
		progress.Results[index].Outcome = MemberCompleted
	}
	progress.NextMemberOrdinal = 0
	progress.UpdatedAt = now
	return progress, nil
}

func (executor *GroupExecutor) validateProgress(value GroupProgress) error {
	if executor == nil || value.OperationID != executor.manifest.OperationID ||
		ids.Validate(ids.KindTask, value.AttemptID) != nil ||
		value.UpdatedAt.IsZero() ||
		value.UpdatedAt.Location() != time.UTC ||
		len(value.Results) > len(executor.manifest.Members) {
		return invalid("release group progress is invalid")
	}
	return nil
}

func sameMember(member GroupMember, result MemberResult) bool {
	return member.Ordinal == result.Ordinal && member.ServiceID == result.ServiceID &&
		member.ReleaseID == result.ReleaseID
}
