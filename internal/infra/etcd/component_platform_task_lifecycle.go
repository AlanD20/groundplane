package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type platformComponentTaskChange struct {
	applies    bool
	conditions []Condition
	mutations  []Mutation
	values     [][]byte
}

func (repository *TaskRepository) preparePlatformComponentTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) (platformComponentTaskChange, error) {
	if task.Params[TaskResourceKindParam] != TaskResourceComponent ||
		ids.Validate(ids.KindComponent, task.Target) != nil {
		return platformComponentTaskChange{}, nil
	}
	stateKeys := []string{
		componentKey(task.Target),
		platformComponentTaskRenderInputKey(task.PlanID),
		platformComponentTaskActiveKey(task.Target),
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: stateKeys, Revision: revision})
	if err != nil {
		return platformComponentTaskChange{}, err
	}
	if state == nil || len(state.Values) != 3 || state.Values[0] == nil || state.Values[1] == nil {
		return platformComponentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"platform Component Task state is incomplete",
		)
	}
	componentValue := state.Values[0]
	renderInputValue := state.Values[1]
	component, err := decodeComponentRecord(componentValue.Value)
	if err != nil {
		return platformComponentTaskChange{}, err
	}
	input, err := decodePlatformComponentTaskRenderInput(renderInputValue.Value)
	if err != nil {
		return platformComponentTaskChange{}, err
	}
	if component.Desired.Owner != core.ComponentOwnerPlatform || component.Desired.OwnerID != "" ||
		(input.TaskID != task.ID && task.RetryOf == "") ||
		input.PlanID != task.PlanID || input.ComponentID != task.Target || task.PlanHash != input.PlanSHA256 ||
		task.Params[TaskPlatformComponentDesiredSHA256Param] != input.DesiredSHA256 {
		return platformComponentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"platform Component Task no longer matches its render input",
		)
	}
	resolverAttempt := task.Actor == TaskActorSystem ||
		task.RetryOf != "" && state.Values[2] != nil && string(state.Values[2].Value) == task.ID
	if task.Actor == TaskActorSystem && !resolverAttempt {
		return platformComponentTaskChange{}, errs.New(errs.KindStateConflict, "platform DNS resolver Task ownership changed")
	}
	desiredSHA256, err := PlatformComponentDesiredDigest(component)
	if err != nil {
		return platformComponentTaskChange{}, err
	}
	if desiredSHA256 != input.DesiredSHA256 {
		return platformComponentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"platform Component desired state changed during reconciliation",
		)
	}

	change := platformComponentTaskChange{
		applies: true,
		conditions: []Condition{
			{Key: componentKey(task.Target), ModRevision: componentValue.ModRevision},
			{Key: platformComponentTaskRenderInputKey(task.PlanID), ModRevision: renderInputValue.ModRevision},
		},
	}
	if resolverAttempt {
		change.conditions = append(change.conditions, Condition{
			Key: platformComponentTaskActiveKey(task.Target), ModRevision: state.Values[2].ModRevision,
		})
	}
	if terminalStatus != TaskStatusCompleted {
		return change, nil
	}
	generatedServices := []string(nil)
	if component.Desired.Enabled {
		generatedServices = []string{input.GeneratedServiceID}
	}
	promoted, err := SetComponentRuntime(
		component,
		generatedServices,
		"",
		component.Desired.Enabled,
	)
	if err != nil {
		return platformComponentTaskChange{}, err
	}
	value, err := encodeComponentRecord(promoted)
	if err != nil {
		return platformComponentTaskChange{}, err
	}
	change.values = append(change.values, value)
	change.mutations = append(change.mutations, Mutation{
		Type:  MutationPut,
		Key:   componentKey(task.Target),
		Value: value,
	}, componentWriteFenceMutation(task.Target))
	return change, nil
}

func (repository *TaskRepository) validatePlatformComponentTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) error {
	if task.Params[TaskResourceKindParam] != TaskResourceComponent ||
		ids.Validate(ids.KindComponent, task.Target) != nil {
		return nil
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys:     []string{platformComponentTaskRenderInputKey(task.PlanID)},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if state == nil || len(state.Values) != 1 || state.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "platform Component Task render input is missing")
	}
	input, err := decodePlatformComponentTaskRenderInput(state.Values[0].Value)
	if err != nil {
		return err
	}
	if (input.TaskID != task.ID && task.RetryOf == "") || input.PlanID != task.PlanID ||
		input.ComponentID != task.Target || task.PlanHash != input.PlanSHA256 ||
		input.DesiredSHA256 != task.Params[TaskPlatformComponentDesiredSHA256Param] {
		return errs.New(errs.KindStateConflict, "platform Component Task replay evidence changed")
	}
	return nil
}

func clearPlatformComponentTaskChange(change platformComponentTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
