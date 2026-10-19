package componentplanning

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ControllerProxySelection captures Service replacement inputs for a proven
// disabled router. It is metadata authority, never an Agent execution recipe.
// Keeping it under the immutable desired revision gives it that revision's
// existing reference retention and protected cleanup lifetime.
type ControllerProxySelection struct {
	ComponentID string                `json:"component_id"`
	WorkItemID  string                `json:"work_item_id"`
	OperationID string                `json:"operation_id"`
	Preflight   ProxyServicePreflight `json:"preflight"`
}

func ControllerProxySelectionKey(environmentID, parentID, componentID string) string {
	return blueprints.EnvironmentBlueprintRevisionPrefixFinal(
		environmentID,
		parentID,
	) + "proxy-selections/" + componentID
}

func (selection ControllerProxySelection) validate() error {
	if ids.Validate(ids.KindComponent, selection.ComponentID) != nil ||
		ids.Validate(
			ids.KindOperation,
			selection.OperationID,
		) != nil || ids.Validate(ids.KindWorkItem, selection.WorkItemID) != nil {
		return errs.New(errs.KindStateConflict, "Controller proxy selection identity is invalid")
	}
	if input := selection.Preflight.CandidateInput; input != nil &&
		(input.SourceOwner != ProxyInputControllerSettlement ||
			input.SourceID != selection.OperationID || input.ComponentID != selection.ComponentID) {
		return errs.New(errs.KindStateConflict, "disabled proxy input differs from its Controller settlement")
	}
	return selection.Preflight.validate()
}

func DecodeControllerProxySelection(value []byte) (ControllerProxySelection, error) {
	selection, err := recordcodec.Decode[ControllerProxySelection](value, "controller-proxy-selection")
	if err != nil {
		return ControllerProxySelection{}, err
	}
	if err := selection.validate(); err != nil {
		return ControllerProxySelection{}, err
	}
	return selection, nil
}

type ControllerProxyPublication struct {
	record     ControllerProxySelection
	conditions []keyvalue.Condition
	mutations  []keyvalue.Mutation
	value      []byte
}

func (authority FrozenProjectionAuthority) HasProxyServicePreflight() bool {
	return authority.proxyServicePreflight != nil
}

func PrepareControllerProxySelection(ctx context.Context, store planningStore,
	work workqueue.View, preflight ProxyServicePreflight, sourceChange workqueue.Change,
	runtimeCondition keyvalue.Condition, headRevision int64,
) (ControllerProxyPublication, error) {
	task, item := work.Task.Task, work.Item.Item
	operationID, err := ControllerProxyOperationID(task, item)
	if err != nil {
		return ControllerProxyPublication{}, err
	}
	parentOperationID, err := ProxySourceOperationID(task)
	if err != nil {
		return ControllerProxyPublication{}, err
	}
	if work.ReadRevision <= 0 || work.Task.Revision <= 0 || work.Item.Revision <= 0 ||
		task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.ForwardRevoked ||
		(item.Phase != workexecution.Waiting && item.Phase != workexecution.Ready) ||
		headRevision <= 0 || runtimeCondition.Key != RuntimeKey(item.TargetID) || runtimeCondition.ModRevision < 0 ||
		preflight.ParentTaskID != task.ID || preflight.EnvironmentID != task.Scope.EnvironmentID {
		return ControllerProxyPublication{}, errs.New(
			errs.KindStateConflict,
			"Disabled proxy publication differs from captured work",
		)
	}
	if hold := preflight.SourceHold; hold != nil && hold.OperationID != parentOperationID {
		return ControllerProxyPublication{}, errs.New(
			errs.KindStateConflict,
			"Disabled proxy hook inputs belong to another Apply",
		)
	}
	publicationRevision, err := proxyPublicationRevision(preflight, sourceChange, work.ReadRevision)
	if err != nil {
		return ControllerProxyPublication{}, err
	}
	if err := validateProxyRolloutPublication(ctx, store, &preflight, sourceChange.Mutations, publicationRevision); err != nil {
		return ControllerProxyPublication{}, err
	}
	record := ControllerProxySelection{ComponentID: item.TargetID, WorkItemID: item.ID,
		OperationID: operationID, Preflight: *cloneProxyServicePreflight(&preflight)}
	if err := record.validate(); err != nil {
		return ControllerProxyPublication{}, err
	}
	if preflight.CandidateInput == nil {
		return ControllerProxyPublication{}, errs.New(
			errs.KindStateConflict,
			"Disabled proxy publication has no captured input",
		)
	}
	inputCompared := false
	for _, condition := range sourceChange.Conditions {
		if condition.Key == ProxyCandidateInputKey(*preflight.CandidateInput) && condition.ModRevision > 0 &&
			condition.ModRevision <= publicationRevision {
			inputCompared = true
		}
	}
	if !inputCompared {
		return ControllerProxyPublication{}, errs.New(
			errs.KindStateConflict,
			"Disabled proxy publication must compare its captured input",
		)
	}
	var rollouts []ProxyServiceRolloutReference
	for _, source := range preflight.Services {
		if source.ReplacementRequired && source.Rollout != nil {
			rollouts = append(rollouts, *source.Rollout)
		}
	}
	_, comparisons, err := BindProxyServiceRollouts(preflight, rollouts)
	if err != nil {
		return ControllerProxyPublication{}, err
	}
	value, err := recordcodec.Encode("controller-proxy-selection", record)
	if err != nil {
		return ControllerProxyPublication{}, err
	}
	key := ControllerProxySelectionKey(preflight.EnvironmentID, task.ID, item.TargetID)
	conditions := append([]keyvalue.Condition(nil), sourceChange.Conditions...)
	conditions = append(conditions, comparisons...)
	conditions = append(
		conditions,
		runtimeCondition,
		keyvalue.Condition{
			Key:         blueprints.EnvironmentBlueprintHeadKey(preflight.EnvironmentID),
			ModRevision: headRevision,
		},
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: work.Item.Revision},
		keyvalue.Condition{Key: key},
	)
	mutations := append(cloneProxySourceMutations(sourceChange.Mutations),
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return ControllerProxyPublication{record: record, value: value, conditions: conditions, mutations: mutations}, nil
}

func (publication ControllerProxyPublication) Validate(
	environmentID, parentID, componentID string,
	implementation serviceproxy.Implementation,
) error {
	if publication.record.Preflight.EnvironmentID != environmentID ||
		publication.record.Preflight.ParentTaskID != parentID ||
		publication.record.ComponentID != componentID ||
		publication.record.Preflight.Implementation != implementation ||
		len(publication.value) == 0 {
		return errs.New(errs.KindStateConflict, "disabled proxy capture differs from its selected Component")
	}
	return publication.record.validate()
}

func (publication ControllerProxyPublication) Conditions() []keyvalue.Condition {
	return publication.conditions
}
func (publication ControllerProxyPublication) Mutations() []keyvalue.Mutation {
	return publication.mutations
}
func (publication *ControllerProxyPublication) Clear() {
	if publication != nil {
		keyvalue.ClearMutationValues(publication.mutations)
		*publication = ControllerProxyPublication{}
	}
}

func ReadControllerProxySelection(ctx context.Context, store planningStore, environmentID, parentID, componentID string,
	revision int64) (keyvalue.Versioned[ControllerProxySelection], bool, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[ControllerProxySelection]{}, false, err
	}
	if store == nil || revision <= 0 || ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindTask, parentID) != nil || ids.Validate(ids.KindComponent, componentID) != nil {
		return keyvalue.Versioned[ControllerProxySelection]{}, false, errs.New(
			errs.KindValidationFailed,
			"Controller proxy capture request is invalid",
		)
	}
	key := ControllerProxySelectionKey(environmentID, parentID, componentID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[ControllerProxySelection]{}, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Versioned[ControllerProxySelection]{}, false, errs.New(
			errs.KindInternal,
			"Controller proxy capture read is incomplete",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return keyvalue.Versioned[ControllerProxySelection]{ReadRevision: revision}, false, nil
	}
	if read.Values[0].Key != key {
		return keyvalue.Versioned[ControllerProxySelection]{}, false, errs.New(
			errs.KindInternal,
			"Controller proxy capture storage identity is invalid",
		)
	}
	record, err := DecodeControllerProxySelection(read.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[ControllerProxySelection]{}, false, err
	}
	if record.ComponentID != componentID || record.Preflight.ParentTaskID != parentID ||
		record.Preflight.EnvironmentID != environmentID {
		return keyvalue.Versioned[ControllerProxySelection]{}, false, errs.New(
			errs.KindStateConflict,
			"Controller proxy capture ownership changed",
		)
	}
	return keyvalue.Versioned[ControllerProxySelection]{
		Record:       record,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: revision,
	}, true, nil
}
