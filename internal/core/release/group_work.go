package release

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// GroupWorkMember binds the declared order to native execution identities.
// Restore is a separate captured operation: a completed forward attempt is
// never rewritten as failed in order to obtain permission to compensate it.
type GroupWorkMember struct {
	Member    GroupMember `json:"member"`
	ForwardID string      `json:"forward_id"`
	RestoreID string      `json:"restore_id"`
}

type GroupWorkManifest struct {
	Manifest GroupManifest     `json:"manifest"`
	TaskID   string            `json:"task_id"`
	Members  []GroupWorkMember `json:"members"`
}

type GroupWorkAction string

const (
	GroupWorkWait    GroupWorkAction = "wait"
	GroupWorkForward GroupWorkAction = "forward"
	GroupWorkRestore GroupWorkAction = "restore"
	GroupWorkSkip    GroupWorkAction = "skip"
	GroupWorkFinish  GroupWorkAction = "finish"
)

// GroupWorkDecision is a Controller decision, not permission for a host effect.
// The owner compares the observed work revisions with the input/settlement write.
// A skipped item requires saved no-effect evidence; it is not a fake attempt.
type GroupWorkDecision struct {
	Action        GroupWorkAction
	WorkID        string
	Outcome       workexecution.Outcome
	Reason        string
	Reads         []string
	SourceWorkID  string
	FailureWorkID string
}

func (manifest GroupWorkManifest) Validate() error {
	if err := ValidateGroupManifest(manifest.Manifest); err != nil {
		return err
	}
	if ids.Validate(ids.KindTask, manifest.TaskID) != nil || len(manifest.Members) != len(manifest.Manifest.Members) {
		return invalid("native Release Group membership is incomplete")
	}
	seen := make(map[string]bool, len(manifest.Members)*2)
	for index, member := range manifest.Members {
		if member.Member != manifest.Manifest.Members[index] ||
			ids.Validate(ids.KindWorkItem, member.ForwardID) != nil || ids.Validate(ids.KindWorkItem, member.RestoreID) != nil ||
			member.ForwardID == member.RestoreID || seen[member.ForwardID] || seen[member.RestoreID] {
			return invalid("native Release Group work identity or order is invalid")
		}
		seen[member.ForwardID], seen[member.RestoreID] = true, true
	}
	return nil
}

// DecideGroupWork derives the next bounded decision from native resource work,
// not a second mutable execution journal. GroupExecutor remains the owner of
// declared-order and reverse-compensation policy. Unknown effects must settle
// before a later member or restoration can proceed.
func DecideGroupWork(manifest GroupWorkManifest, items []workexecution.Item, at time.Time) (GroupWorkDecision, error) {
	if err := manifest.Validate(); err != nil {
		return GroupWorkDecision{}, err
	}
	if at.IsZero() || at.Location() != time.UTC || len(items) != 2*len(manifest.Members) {
		return GroupWorkDecision{}, invalid("native Release Group decision requires one complete work snapshot")
	}
	byID := make(map[string]workexecution.Item, len(items))
	reads := make([]string, 0, len(items))
	for _, item := range items {
		if item.Validate() != nil || item.TaskID != manifest.TaskID || byID[item.ID].ID != "" || at.Before(item.UpdatedAt) {
			return GroupWorkDecision{}, invalid("native Release Group work snapshot is inconsistent")
		}
		byID[item.ID] = item
		reads = append(reads, item.ID)
	}
	for _, member := range manifest.Members {
		forward, forwardFound := byID[member.ForwardID]
		restore, restoreFound := byID[member.RestoreID]
		if !forwardFound || !restoreFound || forward.TargetID != member.Member.ServiceID || restore.TargetID != member.Member.ServiceID ||
			forward.Operation != workexecution.Release || restore.Operation != workexecution.ReleaseRestoration ||
			forward.Placement != restore.Placement || forward.Placement.Executor != workexecution.Agent {
			return GroupWorkDecision{}, invalid("native Release Group work belongs to different members or hosts")
		}
	}
	executor, err := NewGroupExecutor(manifest.Manifest)
	if err != nil {
		return GroupWorkDecision{}, err
	}
	progress, err := executor.Begin(manifest.TaskID, at)
	if err != nil {
		return GroupWorkDecision{}, err
	}
	decision := func(action GroupWorkAction, workID string, outcome workexecution.Outcome, reason string) GroupWorkDecision {
		return GroupWorkDecision{Action: action, WorkID: workID, Outcome: outcome, Reason: reason, Reads: reads}
	}
	failed := -1
	for index, member := range manifest.Members {
		item := byID[member.ForwardID]
		if item.Phase != workexecution.Settled {
			if item.Input == nil && item.AttemptNumber == 0 && item.ActiveAttemptID == "" && !item.ForwardRevoked {
				return decision(GroupWorkForward, item.ID, "", "Previous declared members completed"), nil
			}
			return decision(GroupWorkWait, item.ID, "", "Waiting for the current member and its effects to settle"), nil
		}
		if item.Outcome != workexecution.Succeeded {
			failed = index
			if item.Outcome != workexecution.Failed {
				// Abort/latest-wins cannot manufacture an old restoration grant.
				return finishStoppedGroup(manifest, byID, index, item.Outcome, reads)
			}
			progress, err = executor.RecordFailure(progress, MemberResult{
				Ordinal: member.Member.Ordinal, ServiceID: member.Member.ServiceID, ReleaseID: member.Member.ReleaseID,
				Outcome: MemberFailed, FailureCode: "release.member_failed",
			}, at)
			break
		}
		progress, err = executor.RecordServing(progress, MemberResult{
			Ordinal: member.Member.Ordinal, ServiceID: member.Member.ServiceID, ReleaseID: member.Member.ReleaseID,
			Outcome: MemberServing, ServingReleaseID: member.Member.ReleaseID,
		}, at)
		if err != nil {
			return GroupWorkDecision{}, err
		}
	}
	if err != nil {
		return GroupWorkDecision{}, err
	}
	if failed >= 0 {
		for index := failed + 1; index < len(manifest.Members); index++ {
			item := byID[manifest.Members[index].ForwardID]
			if item.Phase != workexecution.Settled {
				return skipGroupWork(item, workexecution.Failed, "An earlier declared member failed", reads)
			}
			if item.Outcome == workexecution.Succeeded || item.AttemptNumber != 0 {
				return GroupWorkDecision{}, invalid("Release Group executed a member after an earlier failure")
			}
		}
	}
	for progress.Compensating {
		index := int(progress.NextCompensationOrdinal) - 1
		member := manifest.Members[index]
		item := byID[member.RestoreID]
		if item.Phase != workexecution.Settled {
			if item.Input == nil && item.AttemptNumber == 0 && item.ActiveAttemptID == "" && !item.ForwardRevoked {
				selected := decision(GroupWorkRestore, item.ID, "", "Restore the earlier serving member in reverse order")
				selected.SourceWorkID, selected.FailureWorkID = member.ForwardID, manifest.Members[failed].ForwardID
				return selected, nil
			}
			selected := decision(GroupWorkWait, item.ID, "", "Waiting for captured restoration and its effects to settle")
			selected.SourceWorkID, selected.FailureWorkID = member.ForwardID, manifest.Members[failed].ForwardID
			return selected, nil
		}
		outcome := MemberCompensated
		if item.Outcome != workexecution.Succeeded {
			outcome = MemberRecoveryRequired
		}
		progress, err = executor.RecordCompensation(progress, MemberResult{
			Ordinal: member.Member.Ordinal, ServiceID: member.Member.ServiceID, ReleaseID: member.Member.ReleaseID,
			Outcome: outcome,
		}, at)
		if err != nil {
			return GroupWorkDecision{}, err
		}
		if outcome == MemberRecoveryRequired {
			return decision(GroupWorkWait, item.ID, workexecution.Failed, "Restoration failed; preserve remaining captured members for operator recovery"), nil
		}
	}
	for _, member := range manifest.Members {
		item := byID[member.RestoreID]
		if item.Phase != workexecution.Settled {
			return skipGroupWork(item, workexecution.Succeeded, "No restoration is required for this member", reads)
		}
	}
	outcome := workexecution.Succeeded
	if failed >= 0 {
		outcome = workexecution.Failed
	}
	return decision(GroupWorkFinish, "", outcome, "All required member effects are settled"), nil
}

func skipGroupWork(item workexecution.Item, outcome workexecution.Outcome, reason string, reads []string) (GroupWorkDecision, error) {
	if item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Input != nil || item.Cleanup != workexecution.CleanupNone {
		return GroupWorkDecision{}, errs.New(errs.KindStateConflict, "Release Group cannot skip work that obtained executable input or an attempt")
	}
	return GroupWorkDecision{Action: GroupWorkSkip, WorkID: item.ID, Outcome: outcome, Reason: reason, Reads: reads}, nil
}

func finishStoppedGroup(manifest GroupWorkManifest, items map[string]workexecution.Item, failed int,
	outcome workexecution.Outcome, reads []string,
) (GroupWorkDecision, error) {
	for index, member := range manifest.Members {
		if index > failed {
			item := items[member.ForwardID]
			if item.Phase != workexecution.Settled {
				return skipGroupWork(item, outcome, "The Release Group was stopped before this member", reads)
			}
		}
		item := items[member.RestoreID]
		if item.Phase != workexecution.Settled {
			return skipGroupWork(item, outcome, "Stopped work cannot obtain a new restoration grant", reads)
		}
	}
	return GroupWorkDecision{Action: GroupWorkFinish, Outcome: outcome, Reads: reads, Reason: "Stopped Release Group effects are settled"}, nil
}
