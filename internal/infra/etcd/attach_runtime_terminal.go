package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareAcknowledgedAttachTask(
	ctx context.Context, terminal TaskRecord, assignment taskassignments.TaskAssignmentRecord, revision int64,
) (attachTaskChange, error) {
	change, err := repository.prepareAttachTaskAcknowledgement(ctx, terminal, terminal.Status, revision)
	if err != nil || !change.applies || terminal.Status != taskjournal.TaskStatusCompleted {
		return change, err
	}
	input, inputRevision, err := repository.readAttachRuntimePreparation(ctx, terminal, revision)
	if err != nil {
		clearAttachTaskChange(change)
		return attachTaskChange{}, err
	}
	prepared := *input.RuntimePreparation
	change.conditions = append(
		change.conditions,
		etcdstore.Condition{Key: attachTaskRenderInputKey(terminal.PlanID), ModRevision: inputRevision},
	)
	if err := repository.applyBackingHookTerminal(
		ctx, terminal, assignment, input, revision, &change,
	); err != nil {
		clearAttachTaskChange(change)
		return attachTaskChange{}, err
	}
	if len(prepared.Updates) == 0 {
		return change, nil
	}
	result := terminal.Result
	if result == nil || result.Kind != taskjournal.TaskResultCompose || result.ExitCode != 0 ||
		result.Diagnostic != taskjournal.TaskResultDiagnosticNone || result.ReconciliationRequired ||
		result.ExecutionEpoch == 0 || result.ExecutionEpoch != assignment.ExecutionEpoch ||
		terminal.TerminalAssignment == nil || terminal.TerminalAssignment.AssignmentID != assignment.AssignmentID ||
		terminal.TerminalAssignment.AgentID != assignment.AgentID || terminal.FinishedAt == nil {
		clearAttachTaskChange(change)
		return attachTaskChange{}, errs.New(
			errs.KindStateConflict,
			"Attach runtime requires an exact successful assignment",
		)
	}
	update := prepared.Updates[0]
	key := serviceruntimerecord.Key(update.Runtime.ServiceID)
	snapshot, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		clearAttachTaskChange(change)
		return attachTaskChange{}, err
	}
	if snapshot == nil || snapshot.ReadRevision != revision || len(snapshot.Values) != 1 || snapshot.Values[0] == nil ||
		snapshot.Values[0].ModRevision != update.PreviousRevision {
		clearAttachTaskChange(change)
		return attachTaskChange{}, errs.New(
			errs.KindStateConflict,
			"Attach acknowledged runtime changed after preparation",
		)
	}
	defer clearKeyValues(snapshot.Values)
	previous, err := decodeAcknowledgedServiceRuntime(
		snapshot.Values[0].Value,
		input.EnvironmentID,
		update.Runtime.ServiceID,
	)
	if err != nil {
		clearAttachTaskChange(change)
		return attachTaskChange{}, err
	}
	proof, err := json.Marshal(struct {
		Preparation serviceruntimerecord.AttachPreparation `json:"preparation"`
		Result      *taskjournal.TaskResultData            `json:"result"`
	}{prepared, taskjournal.TaskResultToData(result)})
	if err != nil {
		clearAttachTaskChange(change)
		return attachTaskChange{}, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(proof)
	clear(proof)
	record, err := serviceruntimerecord.AcknowledgeAttach(previous, prepared, serviceruntimerecord.Acknowledgement{
		TaskID: terminal.ID, PlanID: terminal.PlanID, PlanHash: terminal.PlanHash, StepID: prepared.StepID,
		AssignmentID: assignment.AssignmentID, AgentID: assignment.AgentID, ExecutionEpoch: assignment.ExecutionEpoch,
		RenderGeneration: uint64(terminal.RenderGeneration), EffectDigest: hex.EncodeToString(digest[:]),
		AcknowledgedAt: *terminal.FinishedAt,
	})
	if err != nil {
		clearAttachTaskChange(change)
		return attachTaskChange{}, err
	}
	value, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", record)
	if err != nil {
		clearAttachTaskChange(change)
		return attachTaskChange{}, err
	}
	change.conditions = append(change.conditions, etcdstore.Condition{Key: key, ModRevision: update.PreviousRevision})
	change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: value})
	change.values = append(change.values, value)
	return change, nil
}

func (repository *TaskRepository) readAttachRuntimePreparation(
	ctx context.Context, task TaskRecord, revision int64,
) (AttachTaskRenderInput, int64, error) {
	snapshot, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{attachTaskRenderInputKey(task.PlanID)}, Revision: revision},
	)
	if err != nil {
		return AttachTaskRenderInput{}, 0, err
	}
	if snapshot == nil || snapshot.ReadRevision != revision || len(snapshot.Values) != 1 || snapshot.Values[0] == nil {
		return AttachTaskRenderInput{}, 0, errs.New(errs.KindStateConflict, "Attach runtime preparation is missing")
	}
	defer clearKeyValues(snapshot.Values)
	input, err := decodeAttachTaskRenderInput(snapshot.Values[0].Value)
	if err != nil {
		return AttachTaskRenderInput{}, 0, err
	}
	if input.AttachID != task.Target || input.EnvironmentID != task.Params[taskjournal.TaskMutationEnvironmentParam] {
		return AttachTaskRenderInput{}, 0, errs.New(
			errs.KindStateConflict,
			"Attach runtime preparation ownership differs",
		)
	}
	return input, snapshot.Values[0].ModRevision, validateAttachRuntimePreparation(input, task)
}

func (repository *TaskRepository) attachRuntimeClaimConditions(
	ctx context.Context, task TaskRecord, revision int64,
) ([]etcdstore.Condition, error) {
	input, inputRevision, err := repository.readAttachRuntimePreparation(ctx, task, revision)
	if err != nil {
		return nil, err
	}
	conditions := []etcdstore.Condition{{Key: attachTaskRenderInputKey(task.PlanID), ModRevision: inputRevision}}
	for _, update := range input.RuntimePreparation.Updates {
		key := serviceruntimerecord.Key(update.Runtime.ServiceID)
		snapshot, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
		if err != nil {
			return nil, err
		}
		if snapshot == nil || snapshot.ReadRevision != revision || len(snapshot.Values) != 1 ||
			snapshot.Values[0] == nil ||
			snapshot.Values[0].ModRevision != update.PreviousRevision {
			return nil, errs.New(errs.KindStateConflict, "Attach acknowledged runtime changed before execution")
		}
		clearKeyValues(snapshot.Values)
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: update.PreviousRevision})
	}
	return conditions, nil
}
