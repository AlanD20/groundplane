package etcd

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	attachoutputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachoutputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func blueprintAttachChildTaskMatches(task TaskRecord, unit blueprintunits.Unit) bool {
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	attachID := task.Params[attachinputs.TaskAttachIDParam]
	return unit.Target.Kind == ids.KindAttach && !unit.Removal && unit.Target.ID == attachID &&
		task.Type == taskjournal.TaskUpdate && task.Actor == taskjournal.TaskActorSystem &&
		task.Executor == taskjournal.TaskExecutorAgent && task.Target == task.Owner.EnvironmentID &&
		task.Params[blueprints.EnvironmentDesiredRevisionParam] == parentID &&
		ids.Validate(ids.KindTask, parentID) == nil && parentID != task.ID &&
		ids.Validate(ids.KindAttach, attachID) == nil && len(task.Params) == 3 &&
		len(task.Steps) == 1 && task.Steps[0].Kind == taskjournal.TaskStepOperation &&
		len(task.Materializations) == 0 && task.EntryRuntime == nil &&
		len(task.ComponentActionStepIDs) == 0 && len(task.ManagedComponentTeardownSources) == 0
}

// prepareBlueprintAttachChildTerminal makes current facts and a create-only
// output generation part of the same transaction as the ready Attach, Task
// terminal journal and verified unit receipt. A RESULT from any other Agent
// assignment or input generation cannot complete this child.
func (repository *TaskRepository) prepareBlueprintAttachChildTerminal(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	revision int64,
	change *blueprintAttachTaskChange,
) error {
	attachID := task.Params[attachinputs.TaskAttachIDParam]
	if !taskjournal.IsBlueprintChild(task.Params) || attachID == "" {
		return nil
	}
	if change == nil || !change.applies {
		return errs.New(errs.KindStateConflict, "Blueprint Attach child intent is unavailable")
	}
	if task.Status != taskjournal.TaskStatusCompleted {
		return nil
	}
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	if task.Result == nil || task.Result.Kind != taskjournal.TaskResultCompose ||
		task.Result.ExitCode != 0 || task.Result.Diagnostic != taskjournal.TaskResultDiagnosticNone ||
		task.Result.ReconciliationRequired || task.Result.ExecutionEpoch == 0 ||
		task.Result.ExecutionEpoch != assignment.ExecutionEpoch ||
		task.TerminalAssignment == nil || task.TerminalAssignment.AssignmentID != assignment.AssignmentID ||
		task.TerminalAssignment.AgentID != assignment.AgentID || task.FinishedAt == nil ||
		task.Type != taskjournal.TaskUpdate || task.Actor != taskjournal.TaskActorSystem ||
		task.Executor != taskjournal.TaskExecutorAgent || task.Target != task.Owner.EnvironmentID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != parentID ||
		ids.Validate(ids.KindTask, parentID) != nil || ids.Validate(ids.KindAttach, attachID) != nil ||
		len(task.Params) != 3 || len(task.Steps) != 1 ||
		task.Steps[0].Kind != taskjournal.TaskStepOperation {
		return errs.New(errs.KindStateConflict, "Blueprint Attach child terminal authority is invalid")
	}
	inputKey := attachinputs.Key(parentID, attachID)
	intentKey := attachrecord.BlueprintAttachTaskIntentKey(task.ID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{inputKey, intentKey, attachrecord.AttachFactsKey(attachID)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 3 ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		if read != nil {
			etcdstore.ClearValues(read.Values)
		}
		return errs.New(errs.KindStateConflict, "Blueprint Attach child terminal authority is unavailable")
	}
	defer etcdstore.ClearValues(read.Values)
	input, err := attachinputs.Decode(read.Values[0].Value)
	if err != nil {
		return err
	}
	defer attachinputs.Clear(&input)
	intent, err := attachrecord.DecodeBlueprintAttachTaskIntent(read.Values[1].Value)
	if err != nil {
		return err
	}
	currentFacts, err := attachrecord.DecodeAttachEncryptedFacts(read.Values[2].Value)
	if err != nil {
		return err
	}
	defer clear(currentFacts.Ciphertext)
	expected, err := attachrecord.NewPendingAttachRecord(
		input.AttachID, input.EnvironmentID, input.AttachName,
		input.BackingProjectID, input.BackingEnvironmentID, input.BackingServiceID,
		input.BackingNetworkID, input.ConsumerServiceID, input.CredentialOwnerID,
		nil, input.FactSets, task.ID, task.CreatedAt,
	)
	if err != nil {
		return err
	}
	expected.HookBundle = true
	if input.OwnerKind != attachinputs.OwnerChild || input.OwnerTaskID != task.ID ||
		input.Transfer == nil || input.Transfer.ParentTaskID != parentID ||
		input.Transfer.ChildTaskID != task.ID || input.ParentTaskID != parentID ||
		input.RevisionID != parentID ||
		input.EnvironmentID != task.Owner.EnvironmentID || input.AttachID != attachID ||
		input.OperationID != task.OperationID || intent.TaskID != task.ID ||
		intent.EnvironmentID != task.Owner.EnvironmentID || len(intent.Candidates) != 1 ||
		!attachrecord.SameBlueprintAttachCandidateRecord(intent.Candidates[0], expected) ||
		!sameBlueprintAttachEncryptedFacts(currentFacts, input.GeneratedInputs) {
		return errs.New(errs.KindStateConflict, "Blueprint Attach child input authority changed")
	}
	checkpoint, checkpointCondition, err := repository.requireBackingHookResultCheckpoint(
		ctx, task, assignment, task.Steps[0].ID, attachID, backinghook.Attach, revision,
	)
	if err != nil {
		return err
	}
	if checkpoint.Facts == nil || checkpoint.Facts.AttachID != attachID {
		return errs.New(errs.KindStateConflict, "Blueprint Attach hook RESULT facts are missing")
	}
	defer clear(checkpoint.Facts.Ciphertext)
	factValue, err := attachrecord.EncodeAttachEncryptedFacts(*checkpoint.Facts)
	if err != nil {
		return err
	}
	output := attachoutputs.Generation{
		EnvironmentID: task.Owner.EnvironmentID, ParentTaskID: parentID,
		ChildTaskID: task.ID, AttachID: attachID, OperationID: task.OperationID,
		AssignmentID: assignment.AssignmentID, ExecutionEpoch: assignment.ExecutionEpoch,
		StepID: task.Steps[0].ID, PlanHash: task.PlanHash,
		ResultSHA256: checkpoint.ResultSHA256, Facts: *checkpoint.Facts,
		CreatedAt: *task.FinishedAt,
	}
	outputValue, err := attachoutputs.Encode(output)
	if err != nil {
		clear(factValue)
		return err
	}
	outputKey := attachoutputs.Key(attachID, task.ID)
	change.conditions = append(change.conditions,
		etcdstore.Condition{Key: inputKey, ModRevision: read.Values[0].ModRevision},
		etcdstore.Condition{Key: attachrecord.AttachFactsKey(attachID), ModRevision: read.Values[2].ModRevision},
		checkpointCondition,
		etcdstore.Condition{Key: outputKey},
	)
	change.mutations = append(change.mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachFactsKey(attachID), Value: factValue},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: outputKey, Value: outputValue},
	)
	change.values = append(change.values, factValue, outputValue)
	change.verifiedChildResult = true
	return nil
}

func sameBlueprintAttachEncryptedFacts(left, right attachrecord.EncryptedFacts) bool {
	return left.AttachID == right.AttachID && left.EnvelopeVersion == right.EnvelopeVersion &&
		left.Cipher == right.Cipher && left.DigestAlgorithm == right.DigestAlgorithm &&
		left.CiphertextSHA256 == right.CiphertextSHA256 && bytes.Equal(left.Ciphertext, right.Ciphertext)
}
