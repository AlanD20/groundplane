package blueprintunits

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

// CapturedGraph contains only work descriptors and references to operation-owned
// inputs. It is immutable and Task-specific, not the Environment's latest plan.
type CapturedGraph struct {
	TaskID          string                 `json:"task_id"`
	EnvironmentID   string                 `json:"environment_id"`
	Items           []workexecution.Item   `json:"items"`
	Units           []Unit                 `json:"units"`
	ServiceRemovals []ServiceRemovalSource `json:"service_removals,omitempty"`
}

// ServiceRemovalSource pins the acknowledged runtime selected during planning.
// Zero means positively proven initial absence, not an optional runtime lookup.
// The full runtime is stored once in the operation input when it becomes ready.
type ServiceRemovalSource struct {
	WorkItemID      string                `json:"work_item_id"`
	ServiceID       string                `json:"service_id"`
	RuntimeRevision int64                 `json:"runtime_revision"`
	RuntimeSHA256   string                `json:"runtime_sha256,omitempty"`
	ClaimRevision   int64                 `json:"claim_revision"`
	Predecessor     *workqueue.ClaimOwner `json:"predecessor,omitempty"`
}

func CapturedGraphKey(environmentID, taskID string) string {
	return recordPrefix + environmentID + "/captured/" + taskID
}

// PrepareCapturedGraph must join Task admission and operation input publication.
// There is no overwrite operation: changed intent always has a new Task identity.
func PrepareCapturedGraph(task workexecution.Task, graph CapturedGraph) (workqueue.Change, error) {
	if task.PublicationComplete || task.Progress.Total() != 0 || validateCapturedGraph(task, graph) != nil {
		return workqueue.Change{}, invalidRecord()
	}
	value, err := recordcodec.Encode("blueprint-captured-work", graph)
	if err != nil {
		return workqueue.Change{}, err
	}
	if len(value) > maximumDesiredPlanBytes {
		clear(value)
		return workqueue.Change{}, invalidRecord()
	}
	key := CapturedGraphKey(graph.EnvironmentID, graph.TaskID)
	conditions := []keyvalue.Condition{{Key: key}}
	for _, source := range graph.ServiceRemovals {
		conditions = append(conditions, keyvalue.Condition{Key: serviceruntimerecord.Key(source.ServiceID), ModRevision: source.RuntimeRevision})
		conditions = append(conditions, keyvalue.Condition{Key: workqueue.ClaimKey(workexecution.Claim{ScopeID: source.ServiceID}), ModRevision: source.ClaimRevision})
	}
	return workqueue.Change{
		Conditions: conditions,
		Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}},
	}, nil
}

func (repository *Repository) LoadCapturedGraph(ctx context.Context, task workexecution.Task, revision int64) (keyvalue.Versioned[CapturedGraph], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[CapturedGraph]{}, err
	}
	if task.Validate() != nil || task.Action != workexecution.ActionApply ||
		task.Scope.EnvironmentID == "" || task.TargetID != task.Scope.EnvironmentID || revision <= 0 {
		return keyvalue.Versioned[CapturedGraph]{}, invalidRecord()
	}
	key := CapturedGraphKey(task.Scope.EnvironmentID, task.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[CapturedGraph]{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Versioned[CapturedGraph]{}, corruptRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision || len(value.Value) > maximumDesiredPlanBytes {
		return keyvalue.Versioned[CapturedGraph]{}, corruptRecord()
	}
	graph, err := recordcodec.Decode[CapturedGraph](value.Value, "blueprint-captured-work")
	if err != nil || validateCapturedGraph(task, graph) != nil {
		return keyvalue.Versioned[CapturedGraph]{}, corruptRecord()
	}
	return keyvalue.Versioned[CapturedGraph]{Record: graph, Revision: value.ModRevision, ReadRevision: revision}, nil
}

func validateCapturedGraph(task workexecution.Task, graph CapturedGraph) error {
	if task.Validate() != nil || task.Action != workexecution.ActionApply ||
		graph.TaskID != task.ID || graph.EnvironmentID == "" || graph.EnvironmentID != task.Scope.EnvironmentID ||
		task.TargetID != graph.EnvironmentID || len(graph.Items) > maximumDesiredUnits || len(graph.Units) != len(graph.Items) {
		return invalidRecord()
	}
	if err := workexecution.ValidateGraph(task, graph.Items); err != nil {
		return err
	}
	if err := validateServiceRemovalSources(graph); err != nil {
		return err
	}
	targets := make(map[ResourceKey]string, len(graph.Items))
	for index, item := range graph.Items {
		unit := graph.Units[index]
		if !validUnit(unit) || item.TargetID != unit.Target.ID || targets[unit.Target] != "" ||
			item.ID != ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "blueprint-resource/"+string(unit.Target.Kind)+"/"+unit.Target.ID) {
			return invalidRecord()
		}
		targets[unit.Target] = item.ID
		for _, write := range unit.Writes {
			if !slices.Contains(item.Claims, workexecution.Claim{ScopeID: write.ID}) {
				return invalidRecord()
			}
		}
		if item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Result != nil || item.FirstFailure != nil ||
			item.Outcome != "" || item.Cleanup != workexecution.CleanupNone ||
			item.Phase != workexecution.Ready && item.Phase != workexecution.Waiting ||
			!item.CreatedAt.Equal(task.CreatedAt) || !item.UpdatedAt.Equal(task.CreatedAt) {
			return invalidRecord()
		}
		value, err := workqueue.EncodeItem(item)
		clear(value)
		if err != nil {
			return err
		}
	}
	for index, item := range graph.Items {
		dependencies := make([]string, 0, len(graph.Units[index].After))
		for _, target := range graph.Units[index].After {
			dependency := targets[target]
			if dependency == "" {
				return invalidRecord()
			}
			dependencies = append(dependencies, dependency)
		}
		slices.Sort(dependencies)
		if !slices.Equal(dependencies, item.Dependencies) {
			return invalidRecord()
		}
	}
	return nil
}
