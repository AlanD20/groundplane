package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Claim publication owns all assignment copies in its existing transaction.
// Hook-bearing Detach retains the same authority; an interrupted hook requires
// a separately recorded operator authorization before it may execute again.
func (repository *TaskRepository) prepareAttachRemovalRecoveryClaim(
	ctx context.Context, task TaskRecord, assignment *taskassignments.TaskAssignmentRecord, revision int64,
) ([]keyvalue.Condition, []byte, error) {
	authority, conditions, found, err := repository.attachRemovalRecoveryAuthorityAtRevision(ctx, task, revision)
	if err != nil || !found {
		return nil, nil, err
	}
	digest, err := taskassignments.AttachRemovalRecoveryAuthoritySHA256(authority)
	if err != nil {
		return nil, nil, err
	}
	next := *assignment
	next.AttachRemovalAuthoritySHA256 = digest
	encoded, err := taskassignments.EncodeTaskAssignment(next)
	if err != nil {
		return nil, nil, err
	}
	*assignment = next
	return append(conditions, keyvalue.Condition{Key: taskassignments.AttachRemovalRecoveryKey(task.ID)}), encoded, nil
}

// The native intent is retained throughout recovery. No current Blueprint,
// desired backing image or freshly selected credentials can replace it.
func (repository *TaskRepository) attachRemovalRecoveryAuthorityAtRevision(
	ctx context.Context, task TaskRecord, revision int64,
) (taskassignments.AttachRemovalRecoveryAuthority, []keyvalue.Condition, bool, error) {
	if !authoredBlueprintAttachRemovalTask(task) {
		return taskassignments.AttachRemovalRecoveryAuthority{}, nil, false, nil
	}
	value, intent, found, err := repository.readAuthoredAttachRemovalIntent(ctx, task, revision)
	if err != nil || !found {
		return taskassignments.AttachRemovalRecoveryAuthority{}, nil, false, err
	}
	defer clear(value.Value)
	defer environmentchanges.ClearAttachRemovalIntent(&intent)
	if intent.Status != taskjournal.TaskStatusPending || validateAuthoredAttachRemovalTaskOwner(task, intent) != nil {
		return taskassignments.AttachRemovalRecoveryAuthority{}, nil, false, taskassignments.CorruptTaskAssignment()
	}
	stepCount := 1
	if intent.Detaching.OwnsCredential() && intent.Detaching.HookBundle &&
		intent.RenderInput.HookConfiguration != nil && intent.RenderInput.HookConfiguration.Detach != nil {
		stepCount = 2
	} else if intent.Detaching.OwnsCredential() && intent.RenderInput.Authentication != core.BackingAuthenticationNone &&
		intent.RenderInput.AdapterKey != "custom" {
		switch intent.RenderInput.AdapterKey {
		case "postgres", "mysql", "valkey":
			stepCount = len(intent.Detaching.GrantAttachIDs) + 2
		default:
			return taskassignments.AttachRemovalRecoveryAuthority{}, nil, false, taskassignments.CorruptTaskAssignment()
		}
	}
	if len(task.Steps) != stepCount {
		return taskassignments.AttachRemovalRecoveryAuthority{}, nil, false, taskassignments.CorruptTaskAssignment()
	}
	current, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{attachrecord.AttachKey(intent.AttachID)}, Revision: revision,
	})
	if err != nil {
		return taskassignments.AttachRemovalRecoveryAuthority{}, nil, false, err
	}
	if current == nil || current.ReadRevision != revision || len(current.Values) != 1 || current.Values[0] == nil {
		return taskassignments.AttachRemovalRecoveryAuthority{}, nil, false, errs.New(errs.KindStateConflict,
			"Attach removal target is unavailable")
	}
	defer keyvalue.ClearValues(current.Values)
	attach, err := attachrecord.DecodeAttachRecord(current.Values[0].Value)
	if err != nil || attach.Status != core.AttachDetaching || attach.Operation != attachrecord.AttachOperationDetach ||
		attach.TaskID != task.ID || !attachrecord.AttachImmutableEqual(attach, intent.Detaching) {
		return taskassignments.AttachRemovalRecoveryAuthority{}, nil, false, errs.New(errs.KindStateConflict,
			"Attach removal target changed")
	}
	digest := sha256.Sum256(value.Value)
	authority := taskassignments.AttachRemovalRecoveryAuthority{
		TaskID: task.ID, OperationID: task.OperationID, PlanHash: task.PlanHash,
		EnvironmentID: intent.EnvironmentID, AttachID: intent.AttachID,
		BackingServiceID: intent.Source.BackingServiceID, IntentSHA256: hex.EncodeToString(digest[:]),
		IntentRevision: value.ModRevision, StepIDs: make([]string, len(task.Steps)),
	}
	for index, step := range task.Steps {
		authority.StepIDs[index] = step.ID
	}
	if err := taskassignments.ValidateAttachRemovalRecoveryAuthority(authority); err != nil {
		return taskassignments.AttachRemovalRecoveryAuthority{}, nil, false, err
	}
	return authority, []keyvalue.Condition{
		{Key: value.Key, ModRevision: value.ModRevision},
		{Key: current.Values[0].Key, ModRevision: current.Values[0].ModRevision},
	}, true, nil
}

// Only a contiguous prefix of the original assignment can justify a suffix.
// Trimmed checkpoints retain exactly the same execution identity as events.
func (repository *TaskRepository) attachRemovalMutationEvidenceAtRevision(
	ctx context.Context, task TaskRecord, assignment taskassignments.TaskAssignmentRecord,
	authority taskassignments.AttachRemovalRecoveryAuthority, revision int64,
) ([]taskassignments.ReleaseRecoveryMutationEvidence, error) {
	if authority.TaskID != task.ID || assignment.TaskID != task.ID || assignment.ExecutionEpoch == 0 ||
		assignment.ExecutionMode != taskassignments.TaskExecutionModeForward {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	snapshot, err := repository.ListTaskEvents(ctx, task.ID, revision)
	if err != nil {
		return nil, err
	}
	if snapshot.Revision != revision || snapshot.Task.NextEventSequence != task.NextEventSequence {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	evidence := make([]taskassignments.ReleaseRecoveryMutationEvidence, len(authority.StepIDs))
	for index, stepID := range authority.StepIDs {
		evidence[index].StepID = stepID
	}
	observe := func(identity taskjournal.TaskEventIdentity, running, completed bool) error {
		if identity.TaskID != task.ID || identity.AssignmentID != assignment.AssignmentID ||
			identity.AgentID != assignment.AgentID || identity.AgentGeneration != assignment.AgentGeneration ||
			identity.Attempt == 0 || identity.Attempt > assignment.ExecutionEpoch {
			return taskassignments.CorruptTaskAssignment()
		}
		index := slices.Index(authority.StepIDs, identity.StepID)
		if index < 0 {
			return taskassignments.CorruptTaskAssignment()
		}
		evidence[index].Running = evidence[index].Running || running
		evidence[index].Completed = evidence[index].Completed || completed
		return nil
	}
	for _, checkpoint := range snapshot.Task.EventCheckpoints {
		if err := observe(checkpoint.Identity, checkpoint.Running || checkpoint.EffectPossible,
			checkpoint.Completed); err != nil {
			return nil, err
		}
	}
	for _, event := range snapshot.Events {
		if err := observe(event.Identity, event.State == taskjournal.TaskEventStateRunning,
			event.State == taskjournal.TaskEventStateCompleted); err != nil {
			return nil, err
		}
	}
	end := 0
	for index, item := range evidence {
		if !item.Running {
			if item.Completed {
				return nil, taskassignments.CorruptTaskAssignment()
			}
			continue
		}
		if index != end || index > 0 && !evidence[index-1].Completed {
			return nil, taskassignments.CorruptTaskAssignment()
		}
		end++
	}
	return slices.Clone(evidence[:end]), nil
}
