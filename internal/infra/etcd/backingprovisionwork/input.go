// Package backingprovisionwork owns native backing-facade creation captures.
// Applied resources and configuration sources have independent durable owners.
package backingprovisionwork

import (
	"bytes"
	"slices"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type HookSources struct {
	Pins    *tasksecretpins.Binding      `json:"pins,omitempty"`
	Secrets []tasksecretpinrecord.Record `json:"secrets,omitempty"`
}

// Input contains canonical machine bytes and the non-secret resource/pin
// capture. File-source membership is owned by the exact configuration snapshot
// referenced on the wire, not copied into another mutable file ledger.
type Input struct {
	PlanID      string                      `json:"plan_id"`
	TargetName  string                      `json:"target_name"`
	Wire        []byte                      `json:"wire"`
	Environment hierarchy.EnvironmentRecord `json:"environment"`
	Hook        *HookSources                `json:"hook,omitempty"`
}

func InputKey(workID string) string { return "/v1/execution/backing-provision/inputs/" + workID }

func (input Input) Procedure() (*agentpb.BackingProvisionInput, error) {
	if len(input.Wire) == 0 || len(input.Wire) > backingprovision.MaximumInputBytes || input.TargetName == "" ||
		len(input.TargetName) > 256 || !utf8.ValidString(input.TargetName) || hierarchy.ValidateEnvironment(input.Environment) != nil {
		return nil, invalid("backing creation capture is missing or invalid")
	}
	var procedure agentpb.BackingProvisionInput
	if err := proto.Unmarshal(input.Wire, &procedure); err != nil {
		return nil, invalid("backing creation capture cannot be decoded")
	}
	if err := backingprovision.ValidateInput(input.PlanID, &procedure); err != nil {
		return nil, err
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&procedure)
	if err != nil || !bytes.Equal(canonical, input.Wire) {
		return nil, invalid("backing creation machine input is not canonical")
	}
	environment := input.Environment
	if environment.ID != procedure.EnvironmentId || environment.ProjectID != procedure.ProjectId ||
		environment.VolumeDir != procedure.VolumeDirectory || environment.CreateTaskID != procedure.TaskId ||
		environment.ProvisioningState != hierarchy.EnvironmentProvisioningProvisioning ||
		!environment.CreatedAt.Equal(procedure.CreatedAt.AsTime()) {
		return nil, invalid("backing creation Environment differs from its captured procedure")
	}
	if hook := procedure.AfterStart; hook == nil || hook.EncryptedInputs == nil {
		if input.Hook != nil {
			return nil, invalid("backing creation has hook source retention without captured values")
		}
	} else {
		if input.Hook == nil || len(input.Hook.Secrets) > 128 ||
			(len(input.Hook.Secrets) != 0) != (input.Hook.Pins != nil) {
			return nil, invalid("backing creation hook retention is incomplete")
		}
		if pins := input.Hook.Pins; pins != nil && (!pins.MatchesSources(input.Hook.Secrets) ||
			pins.OperationID != procedure.OperationId || pins.TaskID != procedure.TaskId || pins.WorkItemID != procedure.WorkItemId) {
			return nil, invalid("backing creation hook pins belong to another operation")
		}
		seen := make(map[string]bool, len(input.Hook.Secrets))
		for _, source := range input.Hook.Secrets {
			if tasksecretpinrecord.Validate(source) != nil || source.OperationID != procedure.OperationId || seen[source.SecretID] {
				return nil, invalid("backing creation hook Secret source is invalid or duplicated")
			}
			seen[source.SecretID] = true
		}
	}
	return &procedure, nil
}

func (input Input) Work() (workexecution.Task, workexecution.Item, error) {
	procedure, err := input.Procedure()
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	reference, err := backingprovision.Reference(input.PlanID, procedure)
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	at := procedure.CreatedAt.AsTime()
	task := workexecution.Task{ID: procedure.TaskId, Actor: workexecution.OperatorActor,
		Scope:  workexecution.Scope{ProjectID: procedure.ProjectId, EnvironmentID: procedure.EnvironmentId},
		Action: workexecution.ActionCreate, TargetID: procedure.ServiceId, TargetName: input.TargetName,
		Intent: reference, CreatedAt: at, UpdatedAt: at, Deadline: procedure.Deadline.AsTime(), ExpectedItems: 1}
	item := workexecution.Item{ID: procedure.WorkItemId, TaskID: task.ID, Operation: workexecution.BackingProvision,
		TargetID: task.TargetID, Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: procedure.HostId},
		Phase: workexecution.Ready, Input: &reference, Cleanup: workexecution.CleanupNone,
		CreatedAt: at, UpdatedAt: at, Deadline: task.Deadline}
	for _, id := range append([]string{procedure.EnvironmentId, procedure.ServiceId, procedure.Runtime.Networks[0].NetworkId}, procedure.ManagedVolumeIds...) {
		item.Claims = append(item.Claims, workexecution.Claim{ScopeID: id}, workexecution.Claim{HostID: procedure.HostId, ScopeID: id})
	}
	if err := task.Validate(); err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	return task, item, item.Validate()
}

func (input Input) Matches(task workexecution.Task, item workexecution.Item) error {
	expected, descriptor, err := input.Work()
	if err != nil {
		return err
	}
	if task.Validate() != nil || item.Validate() != nil || task.ID != expected.ID || task.Action != expected.Action ||
		task.Actor != expected.Actor || task.TargetID != expected.TargetID || task.TargetName != expected.TargetName ||
		task.Scope != expected.Scope || task.Intent != expected.Intent || !task.CreatedAt.Equal(expected.CreatedAt) ||
		!task.Deadline.Equal(expected.Deadline) || task.ExpectedItems != 1 || !task.PublicationComplete ||
		item.ID != descriptor.ID || item.TaskID != task.ID || item.TargetID != descriptor.TargetID || item.Operation != descriptor.Operation ||
		item.Placement != descriptor.Placement || item.Input == nil || *item.Input != *descriptor.Input ||
		!item.CreatedAt.Equal(descriptor.CreatedAt) || item.AttemptNumber == 0 && !item.Deadline.Equal(descriptor.Deadline) ||
		!slices.Equal(item.Claims, descriptor.Claims) || len(item.Dependencies) != 0 {
		return invalid("backing creation input differs from its accepted native work")
	}
	return nil
}

func Encode(input Input) ([]byte, error) {
	if _, _, err := input.Work(); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("backing-provision-input", input)
	if err != nil {
		return nil, err
	}
	if len(value) > keyvalue.MaximumBytes {
		clear(value)
		return nil, invalid("backing creation capture exceeds its storage bound")
	}
	return value, nil
}

func Decode(value []byte) (Input, error) {
	if len(value) == 0 || len(value) > keyvalue.MaximumBytes {
		return Input{}, invalid("backing creation capture is missing or oversized")
	}
	input, err := recordcodec.Decode[Input](value, "backing-provision-input")
	if err != nil {
		return Input{}, err
	}
	if _, _, err := input.Work(); err != nil {
		clear(input.Wire)
		return Input{}, err
	}
	return input, nil
}

func invalid(message string) error { return errs.New(errs.KindStateConflict, message) }
