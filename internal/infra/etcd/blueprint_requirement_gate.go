package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"

	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

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
	desiredRevision := task.Params[blueprints.EnvironmentDesiredRevisionParam]
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	validRoot := desiredRevision == gate.DAG.RootTaskID
	if parentID != "" {
		validRoot = ids.Validate(ids.KindTask, parentID) == nil && parentID != task.ID &&
			parentID == desiredRevision && task.Actor == taskjournal.TaskActorSystem
	}
	if gate.TaskID != task.ID || gate.OperationID != task.OperationID || gate.PlanID != task.PlanID ||
		gate.RetryOf != task.RetryOf || task.Type != taskjournal.TaskUpdate || task.Executor != taskjournal.TaskExecutorAgent ||
		!validRoot {
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
	return recordcodec.Encode("blueprint-requirement-gate", gate)
}

func decodeBlueprintRequirementGate(value []byte) (BlueprintRequirementGate, error) {
	gate, err := recordcodec.Decode[BlueprintRequirementGate](value, "blueprint-requirement-gate")
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
) (etcdstore.Versioned[BlueprintRequirementGate], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[BlueprintRequirementGate]{}, false, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil || revision <= 0 {
		return etcdstore.Versioned[BlueprintRequirementGate]{}, false,
			errs.New(errs.KindValidationFailed, "Blueprint requirement gate read is invalid")
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{blueprintRequirementGateKey(taskID)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[BlueprintRequirementGate]{}, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return etcdstore.Versioned[BlueprintRequirementGate]{}, false,
			errs.New(errs.KindInternal, "Blueprint requirement gate read is incomplete")
	}
	if read.Values[0] == nil {
		return etcdstore.Versioned[BlueprintRequirementGate]{ReadRevision: revision}, false, nil
	}
	gate, err := decodeBlueprintRequirementGate(read.Values[0].Value)
	if err != nil {
		return etcdstore.Versioned[BlueprintRequirementGate]{}, false, err
	}
	return etcdstore.Versioned[BlueprintRequirementGate]{
		Record: gate, Revision: read.Values[0].ModRevision, ReadRevision: revision,
	}, true, nil
}

type preparedBlueprintRequirementGatePublication struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

func prepareBlueprintRequirementGatePublication(
	gate BlueprintRequirementGate,
	task TaskRecord,
	projection projectionrecord.EnvironmentComposeProjection,
	required bool,
) (preparedBlueprintRequirementGatePublication, error) {
	marker := task.Params[TaskBlueprintRequirementGateSHA256Param]
	if !required {
		if !gate.IsZero() || marker != "" {
			return preparedBlueprintRequirementGatePublication{}, errs.New(
				errs.KindValidationFailed,
				"unexpected Blueprint requirement gate",
			)
		}
		return preparedBlueprintRequirementGatePublication{}, nil
	}
	if gate.IsZero() || gate.CandidateRevision != projection.BlueprintRequirements.ResolutionRevision ||
		len(gate.DAG.Requirements) != len(projection.BlueprintRequirements.Resolved) {
		return preparedBlueprintRequirementGatePublication{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint requirement gate is incomplete",
		)
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
			return preparedBlueprintRequirementGatePublication{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint requirement gate projection changed",
			)
		}
	}
	value, err := encodeBlueprintRequirementGate(gate)
	if err != nil {
		return preparedBlueprintRequirementGatePublication{}, err
	}
	prepared := preparedBlueprintRequirementGatePublication{
		conditions: make([]etcdstore.Condition, 0, len(gate.DAG.Requirements)+1),
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: blueprintRequirementGateKey(task.ID), Value: value},
		},
	}
	prepared.conditions = append(prepared.conditions, etcdstore.Condition{Key: blueprintRequirementGateKey(task.ID)})
	for _, requirement := range gate.DAG.Requirements {
		prepared.conditions = append(prepared.conditions, etcdstore.Condition{
			Key: attachrecord.AttachKey(requirement.TargetID), ModRevision: requirement.TargetRevision,
		})
	}
	return prepared, nil
}

func (prepared preparedBlueprintRequirementGatePublication) clear() {
	for index := range prepared.mutations {
		clear(prepared.mutations[index].Value)
	}
}
