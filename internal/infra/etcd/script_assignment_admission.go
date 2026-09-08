package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (repository *ScriptRepository) GetScriptExecutionPlan(
	ctx context.Context,
	task TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	executionID := task.Params[ScriptExecutionIDParam]
	if ctx == nil || repository == nil || repository.store == nil || task.Type != TaskScript ||
		!validRawScriptExecutionID(executionID) || len(task.Steps) != 1 {
		return nil, errs.New(errs.KindValidationFailed, "Script execution plan request is invalid")
	}
	read, err := repository.store.Get(ctx, scriptExecutionKey(executionID))
	if err != nil {
		return nil, err
	}
	if read == nil || read.Entry == nil {
		return nil, errs.New(errs.KindStateConflict, "Script execution record is missing")
	}
	record, err := decodeEnvelope[ScriptExecutionRecord](read.Entry.Value, "script-execution")
	if err != nil || validateScriptExecutionRecord(record) != nil || record.CurrentTaskID != task.ID ||
		record.OperationID != task.OperationID || record.StepID != task.Steps[0].ID || record.PlanHash != task.PlanHash {
		return nil, errs.New(errs.KindInternal, "Script execution record is corrupt")
	}
	if _, err := repository.manualScriptExecutionAuthority(ctx, task, record, read.ReadRevision); err != nil {
		return nil, err
	}
	plan := &agentpb.ExecutionPlan{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(record.Plan, plan); err != nil {
		return nil, errs.New(errs.KindInternal, "Script execution plan is corrupt")
	}
	validated, err := executionplan.Validate(plan)
	if err != nil || hex.EncodeToString(validated.PlanHash) != task.PlanHash {
		return nil, errs.New(errs.KindInternal, "Script execution plan is corrupt")
	}
	return validated, nil
}

func (repository *ScriptRepository) GetReleaseScriptExecutionPlan(
	ctx context.Context,
	task TaskRecord,
) (*agentpb.ExecutionPlan, bool, error) {
	if ctx == nil || repository == nil || repository.store == nil ||
		(task.Type != TaskDeploy && task.Type != TaskRollback &&
			(task.Type != TaskUpdate || task.Params[TaskReleasePublicationParam] == "")) {
		return nil, false, errs.New(errs.KindValidationFailed, "release Script execution plan request is invalid")
	}
	executionIDs := make(map[string]string)
	for _, step := range task.Steps {
		if executionID := task.Params[ReleaseHookStepExecutionParam(step.ID)]; executionID != "" {
			if !validRawScriptExecutionID(executionID) {
				return nil, false, errs.New(errs.KindInternal, "release Script execution identity is corrupt")
			}
			executionIDs[step.ID] = executionID
		}
	}
	if len(executionIDs) == 0 {
		return nil, false, nil
	}
	var sealed *agentpb.ExecutionPlan
	var sealedBytes []byte
	for stepID, executionID := range executionIDs {
		read, err := repository.store.Get(ctx, scriptExecutionKey(executionID))
		if err != nil {
			return nil, false, err
		}
		if read == nil || read.Entry == nil {
			return nil, false, errs.New(errs.KindStateConflict, "release Script execution record is missing")
		}
		record, err := decodeEnvelope[ScriptExecutionRecord](read.Entry.Value, "script-execution")
		if err != nil || validateScriptExecutionRecord(record) != nil || record.ID != executionID ||
			record.CurrentTaskID != task.ID || record.OperationID != task.OperationID || record.StepID != stepID ||
			record.PlanHash != task.PlanHash {
			return nil, false, errs.New(errs.KindInternal, "release Script execution record is corrupt")
		}
		if task.Type == TaskUpdate {
			if authorityErr := repository.validateBlueprintScriptExecutionAuthority(
				ctx, task, record, read.ReadRevision,
			); authorityErr != nil {
				return nil, false, authorityErr
			}
		}
		if sealed == nil {
			sealed = &agentpb.ExecutionPlan{}
			if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(record.Plan, sealed); err != nil {
				return nil, false, errs.New(errs.KindInternal, "release Script execution plan is corrupt")
			}
			validated, err := executionplan.Validate(sealed)
			if err != nil || hex.EncodeToString(validated.PlanHash) != task.PlanHash {
				return nil, false, errs.New(errs.KindInternal, "release Script execution plan is corrupt")
			}
			sealed = validated
			sealedBytes = append([]byte(nil), record.Plan...)
		} else if !bytes.Equal(record.Plan, sealedBytes) {
			return nil, false, errs.New(errs.KindInternal, "release Script execution plans disagree")
		}
	}
	for _, step := range sealed.Steps {
		if step.GetRunScript() != nil && executionIDs[step.StepId] != step.GetRunScript().ScriptExecutionId {
			return nil, false, errs.New(errs.KindInternal, "release Script execution plan authority is incomplete")
		}
	}
	return sealed, true, nil
}

func scriptAssignmentTaskOwnsPlan(task TaskRecord, plan *agentpb.ExecutionPlan) bool {
	if task.Type == TaskScript || task.Type == TaskDeploy || task.Type == TaskRollback {
		return true
	}
	return task.Type == TaskUpdate && task.Params[TaskReleasePublicationParam] != "" &&
		plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY && plan.TargetId == task.Target
}

// ResolveScriptAssignmentArtifacts returns the private body bytes only after
// the durable execution, sealed plan, and immutable generation agree.
func (repository *ScriptRepository) ResolveScriptAssignmentArtifacts(
	ctx context.Context,
	task TaskRecord,
	plan *agentpb.ExecutionPlan,
) (*agentpb.ScriptAssignmentArtifacts, error) {
	validated, err := executionplan.Validate(plan)
	if err != nil {
		return nil, err
	}
	if ctx == nil || repository == nil || repository.store == nil || len(validated.ScriptBodyArtifacts) == 0 ||
		!scriptAssignmentTaskOwnsPlan(task, validated) ||
		hex.EncodeToString(validated.PlanHash) != task.PlanHash {
		return nil, errs.New(errs.KindValidationFailed, "Script assignment artifact request is invalid")
	}
	steps := make(map[string]string, len(validated.ScriptBodyArtifacts))
	for _, step := range validated.Steps {
		if run := step.GetRunScript(); run != nil {
			steps[run.ScriptExecutionId] = step.StepId
		}
	}
	artifacts := &agentpb.ScriptAssignmentArtifacts{}
	for _, metadata := range validated.ScriptBodyArtifacts {
		executionRead, readErr := repository.store.Get(ctx, scriptExecutionKey(metadata.ScriptExecutionId))
		if readErr != nil {
			return nil, readErr
		}
		if executionRead == nil || executionRead.Entry == nil {
			return nil, errs.New(errs.KindStateConflict, "Script execution record is missing")
		}
		execution, decodeErr := decodeEnvelope[ScriptExecutionRecord](executionRead.Entry.Value, "script-execution")
		if decodeErr != nil || validateScriptExecutionRecord(execution) != nil || execution.CurrentTaskID != task.ID ||
			execution.OperationID != task.OperationID || execution.StepID != steps[metadata.ScriptExecutionId] ||
			execution.PlanHash != task.PlanHash || !execution.ActiveReference {
			return nil, errs.New(errs.KindInternal, "Script execution record is corrupt")
		}
		if task.Type == TaskScript {
			if _, err := repository.manualScriptExecutionAuthority(ctx, task, execution, executionRead.ReadRevision); err != nil {
				return nil, err
			}
		}
		bodyRead, readErr := repository.store.Get(ctx, scriptSetBodyGenerationKey(
			execution.EnvironmentID, execution.ScriptSetGeneration, metadata.ScriptId, metadata.Generation,
		))
		if readErr != nil {
			return nil, readErr
		}
		if bodyRead == nil || bodyRead.Entry == nil {
			return nil, errs.New(errs.KindInternal, "Script body generation is missing")
		}
		body, decodeErr := decodeScriptBodyGeneration(bodyRead.Entry.Value)
		if decodeErr != nil || body.ScriptID != metadata.ScriptId || body.Generation != metadata.Generation ||
			body.BodySize != metadata.Size || body.BodySHA256 != hex.EncodeToString(metadata.Sha256) ||
			execution.BodySHA256 != body.BodySHA256 {
			return nil, errs.New(errs.KindInternal, "Script body generation does not match its execution")
		}
		ownedBody := []byte(body.Body)
		digest := sha256.Sum256(ownedBody)
		if len(ownedBody) != int(metadata.Size) || hex.EncodeToString(digest[:]) != body.BodySHA256 {
			clear(ownedBody)
			return nil, errs.New(errs.KindInternal, "Script body generation content is corrupt")
		}
		artifacts.Bodies = append(artifacts.Bodies, &agentpb.ScriptBodyArtifact{
			Metadata: proto.Clone(metadata).(*agentpb.ScriptBodyArtifactMetadata), Body: ownedBody,
		})
	}
	return artifacts, nil
}
