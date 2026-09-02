package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	blueprintRequirementGatePrefix          = "/v1/runtime/blueprint-requirement-gates/"
	TaskBlueprintRequirementGateSHA256Param = "blueprint_requirement_gate_sha256"
)

type BlueprintRequirementGate struct {
	Schema            uint8                        `json:"schema"`
	TaskID            string                       `json:"task_id"`
	RetryOf           string                       `json:"retry_of,omitempty"`
	OperationID       string                       `json:"operation_id"`
	PlanID            string                       `json:"plan_id"`
	CandidateRevision int64                        `json:"candidate_revision"`
	DAG               core.BlueprintRequirementDAG `json:"dag"`
	DAGDigest         string                       `json:"dag_sha256"`
}

func NewBlueprintRequirementGate(
	task TaskRecord,
	candidateRevision int64,
	dag core.BlueprintRequirementDAG,
) (BlueprintRequirementGate, error) {
	digest, err := blueprintRequirementDAGDigest(dag)
	if err != nil {
		return BlueprintRequirementGate{}, err
	}
	gate := BlueprintRequirementGate{
		Schema: 1, TaskID: task.ID, OperationID: task.OperationID, PlanID: task.PlanID,
		CandidateRevision: candidateRevision, DAG: dag.Clone(), DAGDigest: digest,
	}
	if task.RetryOf != "" {
		return BlueprintRequirementGate{}, errs.New(errs.KindValidationFailed, "Blueprint requirement gate must originate on its root Task")
	}
	if err := gate.validateTaskIdentity(task, false); err != nil {
		return BlueprintRequirementGate{}, err
	}
	return gate, nil
}

func (gate BlueprintRequirementGate) IsZero() bool {
	return gate.Schema == 0 && gate.TaskID == "" && gate.RetryOf == "" &&
		gate.OperationID == "" && gate.PlanID == "" && gate.CandidateRevision == 0 &&
		gate.DAG.RootTaskID == "" && len(gate.DAG.Requirements) == 0 &&
		len(gate.DAG.TaskEdges) == 0 && len(gate.DAG.OrderedTaskIDs) == 0 &&
		len(gate.DAG.StepIDs) == 0 && len(gate.DAG.PhasePlans) == 0 && gate.DAGDigest == ""
}

func (gate BlueprintRequirementGate) Clone() BlueprintRequirementGate {
	gate.DAG = gate.DAG.Clone()
	return gate
}

func (gate BlueprintRequirementGate) validate() error {
	if gate.Schema != 1 || ids.Validate(ids.KindTask, gate.TaskID) != nil ||
		ids.Validate(ids.KindOperation, gate.OperationID) != nil ||
		ids.Validate(ids.KindPlan, gate.PlanID) != nil || gate.CandidateRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "Blueprint requirement gate identity is invalid")
	}
	if gate.RetryOf == "" {
		if gate.TaskID != gate.DAG.RootTaskID {
			return errs.New(errs.KindValidationFailed, "Blueprint requirement gate root is invalid")
		}
	} else if ids.Validate(ids.KindTask, gate.RetryOf) != nil || gate.RetryOf == gate.TaskID {
		return errs.New(errs.KindValidationFailed, "Blueprint requirement retry gate is invalid")
	}
	if err := gate.DAG.Validate(); err != nil {
		return err
	}
	digest, err := blueprintRequirementDAGDigest(gate.DAG)
	if err != nil || digest != gate.DAGDigest {
		return errs.New(errs.KindValidationFailed, "Blueprint requirement gate digest is invalid")
	}
	return nil
}

func (gate BlueprintRequirementGate) validateTaskIdentity(task TaskRecord, requireMarker bool) error {
	if err := gate.validate(); err != nil {
		return err
	}
	if gate.TaskID != task.ID || gate.OperationID != task.OperationID || gate.PlanID != task.PlanID ||
		gate.RetryOf != task.RetryOf || task.Type != TaskUpdate || task.Executor != TaskExecutorAgent ||
		task.Params[EnvironmentDesiredRevisionParam] != gate.DAG.RootTaskID {
		return errs.New(errs.KindValidationFailed, "Blueprint requirement gate does not match its Task")
	}
	if requireMarker && task.Params[TaskBlueprintRequirementGateSHA256Param] != gate.DAGDigest {
		return errs.New(errs.KindValidationFailed, "Blueprint requirement gate digest does not match its Task")
	}
	return nil
}

func (gate BlueprintRequirementGate) validateForTask(task TaskRecord) error {
	return gate.validateTaskIdentity(task, true)
}

func blueprintRequirementDAGDigest(dag core.BlueprintRequirementDAG) (string, error) {
	value, err := json.Marshal(dag)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:]), nil
}

func encodeBlueprintRequirementGate(gate BlueprintRequirementGate) ([]byte, error) {
	if err := gate.validate(); err != nil {
		return nil, err
	}
	return encodeEnvelope("blueprint-requirement-gate", gate)
}

func decodeBlueprintRequirementGate(value []byte) (BlueprintRequirementGate, error) {
	gate, err := decodeEnvelope[BlueprintRequirementGate](value, "blueprint-requirement-gate")
	if err != nil || gate.validate() != nil {
		return BlueprintRequirementGate{}, corruptBlueprintRequirementGate()
	}
	return gate, nil
}

func corruptBlueprintRequirementGate() error {
	return errs.New(errs.KindInternal, "Blueprint requirement gate durable state is corrupt")
}

func blueprintRequirementGateKey(taskID string) string {
	return blueprintRequirementGatePrefix + taskID
}

func (repository *HierarchyRepository) GetBlueprintRequirementGateAtRevision(
	ctx context.Context,
	taskID string,
	revision int64,
) (Versioned[BlueprintRequirementGate], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[BlueprintRequirementGate]{}, false, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil || revision <= 0 {
		return Versioned[BlueprintRequirementGate]{}, false,
			errs.New(errs.KindValidationFailed, "Blueprint requirement gate read is invalid")
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{blueprintRequirementGateKey(taskID)}, Revision: revision,
	})
	if err != nil {
		return Versioned[BlueprintRequirementGate]{}, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return Versioned[BlueprintRequirementGate]{}, false,
			errs.New(errs.KindInternal, "Blueprint requirement gate read is incomplete")
	}
	if read.Values[0] == nil {
		return Versioned[BlueprintRequirementGate]{ReadRevision: revision}, false, nil
	}
	gate, err := decodeBlueprintRequirementGate(read.Values[0].Value)
	if err != nil {
		return Versioned[BlueprintRequirementGate]{}, false, err
	}
	return Versioned[BlueprintRequirementGate]{
		Record: gate, Revision: read.Values[0].ModRevision, ReadRevision: revision,
	}, true, nil
}

type preparedBlueprintRequirementGatePublication struct {
	conditions []Condition
	mutations  []Mutation
}

func prepareBlueprintRequirementGatePublication(
	gate BlueprintRequirementGate,
	task TaskRecord,
	projection EnvironmentComposeProjection,
	required bool,
) (preparedBlueprintRequirementGatePublication, error) {
	marker := task.Params[TaskBlueprintRequirementGateSHA256Param]
	if !required {
		if !gate.IsZero() || marker != "" {
			return preparedBlueprintRequirementGatePublication{}, errs.New(errs.KindValidationFailed, "unexpected Blueprint requirement gate")
		}
		return preparedBlueprintRequirementGatePublication{}, nil
	}
	if gate.IsZero() || gate.CandidateRevision != projection.BlueprintRequirements.ResolutionRevision ||
		len(gate.DAG.Requirements) != len(projection.BlueprintRequirements.Resolved) {
		return preparedBlueprintRequirementGatePublication{}, errs.New(errs.KindValidationFailed, "Blueprint requirement gate is incomplete")
	}
	if err := gate.validateForTask(task); err != nil {
		return preparedBlueprintRequirementGatePublication{}, err
	}
	for index, requirement := range projection.BlueprintRequirements.Resolved {
		sealed := gate.DAG.Requirements[index]
		if sealed.TargetID != requirement.Target.ID ||
			sealed.TargetTaskID != requirement.Target.TaskID ||
			sealed.TargetRevision != requirement.Target.Revision ||
			sealed.Condition != requirement.Condition ||
			!slices.Equal(sealed.Phases, requirement.Phases) {
			return preparedBlueprintRequirementGatePublication{}, errs.New(errs.KindValidationFailed, "Blueprint requirement gate projection changed")
		}
	}
	value, err := encodeBlueprintRequirementGate(gate)
	if err != nil {
		return preparedBlueprintRequirementGatePublication{}, err
	}
	prepared := preparedBlueprintRequirementGatePublication{
		conditions: make([]Condition, 0, len(gate.DAG.Requirements)+1),
		mutations:  []Mutation{{Type: MutationPut, Key: blueprintRequirementGateKey(task.ID), Value: value}},
	}
	prepared.conditions = append(prepared.conditions, Condition{Key: blueprintRequirementGateKey(task.ID)})
	for _, requirement := range gate.DAG.Requirements {
		prepared.conditions = append(prepared.conditions, Condition{
			Key: attachKey(requirement.TargetID), ModRevision: requirement.TargetRevision,
		})
	}
	return prepared, nil
}

func (prepared preparedBlueprintRequirementGatePublication) clear() {
	for index := range prepared.mutations {
		clear(prepared.mutations[index].Value)
	}
}

func (prepared preparedBlueprintRequirementGatePublication) classify(values []*KeyValue) error {
	if len(values) != len(prepared.conditions) {
		return errs.New(errs.KindInternal, "Blueprint requirement gate compare evidence is incomplete")
	}
	if len(values) == 0 {
		return nil
	}
	if values[0] != nil {
		return errs.New(errs.KindInternal, "Blueprint requirement gate collided with durable state")
	}
	for index, condition := range prepared.conditions[1:] {
		value := values[index+1]
		if value == nil || value.ModRevision != condition.ModRevision {
			return errs.New(errs.KindStateConflict, "Blueprint requirement target changed before publication")
		}
	}
	return nil
}

type blueprintRequirementGateClaimEvidence struct {
	conditions []Condition
}

func (repository *TaskRepository) observeBlueprintRequirementGateForClaim(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (blueprintRequirementGateClaimEvidence, bool, bool, error) {
	desiredRevision := task.Params[EnvironmentDesiredRevisionParam]
	if task.Type != TaskUpdate || task.Executor != TaskExecutorAgent || desiredRevision == "" {
		return blueprintRequirementGateClaimEvidence{}, false, true, nil
	}
	gateKey := blueprintRequirementGateKey(task.ID)
	read, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{gateKey}, Revision: revision,
	})
	if err != nil {
		return blueprintRequirementGateClaimEvidence{}, false, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return blueprintRequirementGateClaimEvidence{}, false, false,
			errs.New(errs.KindInternal, "Blueprint requirement gate claim read is incomplete")
	}
	marker := task.Params[TaskBlueprintRequirementGateSHA256Param]
	if read.Values[0] == nil {
		if marker != "" {
			return blueprintRequirementGateClaimEvidence{}, true, false, corruptBlueprintRequirementGate()
		}
		return blueprintRequirementGateClaimEvidence{}, false, true, nil
	}
	if marker == "" {
		return blueprintRequirementGateClaimEvidence{}, true, false, corruptBlueprintRequirementGate()
	}
	gate, err := decodeBlueprintRequirementGate(read.Values[0].Value)
	if err != nil || gate.validateForTask(task) != nil {
		return blueprintRequirementGateClaimEvidence{}, true, false, corruptBlueprintRequirementGate()
	}
	evidence := blueprintRequirementGateClaimEvidence{
		conditions: []Condition{{Key: gateKey, ModRevision: read.Values[0].ModRevision}},
	}
	for _, requirement := range gate.DAG.Requirements {
		attachRead, readErr := repository.store.GetMany(ctx, GetManyRequest{
			Keys: []string{attachKey(requirement.TargetID)}, Revision: revision,
		})
		if readErr != nil {
			return blueprintRequirementGateClaimEvidence{}, true, false, readErr
		}
		if attachRead == nil || attachRead.ReadRevision != revision || len(attachRead.Values) != 1 {
			return blueprintRequirementGateClaimEvidence{}, true, false,
				errs.New(errs.KindInternal, "Blueprint requirement Attach observation is incomplete")
		}
		attachValue := attachRead.Values[0]
		if attachValue == nil {
			return blueprintRequirementGateClaimEvidence{}, true, false, nil
		}
		attach, decodeErr := decodeAttachRecord(attachValue.Value)
		if decodeErr != nil || attach.ID != requirement.TargetID || attach.TaskID != requirement.TargetTaskID {
			return blueprintRequirementGateClaimEvidence{}, true, false, corruptBlueprintRequirementGate()
		}
		evidence.conditions = append(evidence.conditions, Condition{
			Key: attachKey(attach.ID), ModRevision: attachValue.ModRevision,
		})
		switch requirement.Condition {
		case core.RequirementExists:
		case core.RequirementReady:
			if attach.Status != core.AttachReady {
				return blueprintRequirementGateClaimEvidence{}, true, false, nil
			}
		case core.RequirementCompletedSuccessfully:
			taskRead, taskErr := repository.store.GetMany(ctx, GetManyRequest{
				Keys: []string{taskKey(attach.TaskID)}, Revision: revision,
			})
			if taskErr != nil {
				return blueprintRequirementGateClaimEvidence{}, true, false, taskErr
			}
			if taskRead == nil || taskRead.ReadRevision != revision || len(taskRead.Values) != 1 {
				return blueprintRequirementGateClaimEvidence{}, true, false,
					errs.New(errs.KindInternal, "Blueprint requirement producer Task observation is incomplete")
			}
			if taskRead.Values[0] == nil {
				return blueprintRequirementGateClaimEvidence{}, true, false, nil
			}
			producer, decodeErr := decodeTaskRecord(taskRead.Values[0].Value)
			if decodeErr != nil || producer.ID != attach.TaskID {
				return blueprintRequirementGateClaimEvidence{}, true, false, corruptBlueprintRequirementGate()
			}
			if producer.Status != TaskStatusCompleted {
				return blueprintRequirementGateClaimEvidence{}, true, false, nil
			}
			evidence.conditions = append(evidence.conditions, Condition{
				Key: taskKey(producer.ID), ModRevision: taskRead.Values[0].ModRevision,
			})
		default:
			return blueprintRequirementGateClaimEvidence{}, true, false, corruptBlueprintRequirementGate()
		}
	}
	return evidence, true, true, nil
}

func (repository *TaskRepository) prepareBlueprintRequirementGateRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (releaseTaskRetryChange, error) {
	read, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{blueprintRequirementGateKey(source.ID)}, Revision: revision,
	})
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return releaseTaskRetryChange{}, corruptBlueprintRequirementGate()
	}
	marked := source.Params[TaskBlueprintRequirementGateSHA256Param] != ""
	if read.Values[0] == nil {
		if marked {
			return releaseTaskRetryChange{}, corruptBlueprintRequirementGate()
		}
		return releaseTaskRetryChange{}, nil
	}
	if !marked {
		return releaseTaskRetryChange{}, corruptBlueprintRequirementGate()
	}
	gate, err := decodeBlueprintRequirementGate(read.Values[0].Value)
	if err != nil || gate.validateForTask(source) != nil {
		return releaseTaskRetryChange{}, corruptBlueprintRequirementGate()
	}
	retryGate := gate.Clone()
	retryGate.TaskID = retry.ID
	retryGate.RetryOf = source.ID
	if err := retryGate.validateForTask(retry); err != nil {
		return releaseTaskRetryChange{}, corruptBlueprintRequirementGate()
	}
	value, err := encodeBlueprintRequirementGate(retryGate)
	if err != nil {
		return releaseTaskRetryChange{}, err
	}
	return releaseTaskRetryChange{
		applies: true,
		conditions: []Condition{
			{Key: blueprintRequirementGateKey(source.ID), ModRevision: read.Values[0].ModRevision},
			{Key: blueprintRequirementGateKey(retry.ID)},
		},
		mutations: []Mutation{{
			Type: MutationPut, Key: blueprintRequirementGateKey(retry.ID), Value: value,
		}},
	}, nil
}
