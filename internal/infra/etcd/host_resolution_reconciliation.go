package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// hostResolutionReconciliationChange is deliberately a persistence-only
// contribution. It is joined to the owning Route/Component terminal
// transaction; no handler or scheduler writes the projection directly.
type hostResolutionReconciliationChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

const platformComponentTaskActivePrefix = "/v1/indexes/platform-component-tasks/by-component/"

func platformComponentTaskActiveKey(componentID string) string {
	return platformComponentTaskActivePrefix + componentID
}

func newPlatformDNSResolverTask(componentID string, createdAt time.Time) TaskRecord {
	return TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: taskjournal.PlatformTaskOwner(), Actor: taskjournal.TaskActorSystem, Executor: taskjournal.TaskExecutorAgent,
		PlanID: ids.New(ids.KindPlan), RenderGeneration: 1, Type: taskjournal.TaskUpdate, Target: componentID,
		Params: map[string]string{
			taskjournal.TaskResourceKindParam: taskjournal.TaskResourceComponent,
			TaskAutomaticReconcileParam:       "true",
		},
		Steps: []taskjournal.TaskStepRecord{
			{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)}, {Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)},
		}, TimeoutSeconds: 480,
		Status: taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}
}

func platformResolverTaskSteps(input PlatformComponentTaskRenderInput) []taskjournal.TaskStepRecord {
	count := 2
	if input.EnsureService {
		count = 4
	} else if input.DisableService {
		count = 2
	}
	steps := make([]taskjournal.TaskStepRecord, count)
	for index := range steps {
		steps[index] = taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)}
	}
	return steps
}

func isPlatformDNSResolverTask(task TaskRecord) bool {
	return task.Owner == taskjournal.PlatformTaskOwner() && task.Actor == taskjournal.TaskActorSystem &&
		task.Executor == taskjournal.TaskExecutorAgent && task.Type == taskjournal.TaskUpdate &&
		task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceComponent &&
		task.Params[TaskAutomaticReconcileParam] == "true" &&
		ids.Validate(ids.KindComponent, task.Target) == nil
}

func isPlatformDNSResolverTaskAttempt(task TaskRecord) bool {
	if task.Owner != taskjournal.PlatformTaskOwner() || task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskUpdate ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceComponent ||
		ids.Validate(ids.KindComponent, task.Target) != nil {
		return false
	}
	return task.Actor == taskjournal.TaskActorOperator ||
		task.Actor == taskjournal.TaskActorSystem && task.Params[TaskAutomaticReconcileParam] == "true"
}

func clearHostResolutionReconciliationChange(change hostResolutionReconciliationChange) {
	for _, value := range change.values {
		clear(value)
	}
}

// prepareHostResolutionReconciliation scans the complete Route collection at
// one journal revision and overlays the terminal change being committed. The
// projection therefore never combines Route and Component state from
// different MVCC views.
func (repository *TaskRepository) prepareHostResolutionReconciliation(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
	baseConditions []etcdstore.Condition,
	platformChange platformComponentTaskChange,
) (hostResolutionReconciliationChange, error) {
	resource := task.Params[taskjournal.TaskResourceKindParam]
	if resource != taskjournal.TaskResourceRoute && resource != taskjournal.TaskResourceComponent {
		return hostResolutionReconciliationChange{}, nil
	}
	if revision <= 0 {
		return hostResolutionReconciliationChange{}, errs.New(errs.KindInternal, "host-resolution revision is invalid")
	}
	routes, err := repository.scanRoutesAtRevision(ctx, revision)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	removeRouteID, routeOverride, componentOverride, err :=
		repository.hostResolutionTerminalOverlay(ctx, task, terminalStatus, revision)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if platformChange.promoted != nil {
		componentOverride[platformChange.promoted.Desired.ID] = *platformChange.promoted
	}
	providerIDs := make(map[string]struct{})
	for _, scanned := range routes {
		if scanned.record.Observed.Status == routerecord.ObservedServed {
			providerIDs[scanned.record.Observed.Provider.ComponentID] = struct{}{}
		}
	}
	for _, route := range routeOverride {
		if route.Observed.Status == routerecord.ObservedServed {
			providerIDs[route.Observed.Provider.ComponentID] = struct{}{}
		}
	}
	components, componentConditions, err := repository.hostResolutionComponents(ctx, providerIDs, revision)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	for id, record := range componentOverride {
		components[id] = record
	}
	hostRoutes := make([]resolutionrecord.HostResolutionRouteRecord, 0, len(routes))
	conditions := append([]etcdstore.Condition(nil), baseConditions...)
	for _, condition := range componentConditions {
		conditions = appendHostResolutionCondition(conditions, condition)
	}
	for _, scanned := range routes {
		route := scanned.record
		if route.Desired.ID == removeRouteID {
			continue
		}
		if replacement, found := routeOverride[route.Desired.ID]; found {
			route = replacement
		}
		if route.Desired.Host == "" || route.Observed.Status != routerecord.ObservedServed {
			continue
		}
		provider := components[route.Observed.Provider.ComponentID]
		if !validHostResolutionProvider(provider) {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"served Route provider is not enabled, healthy, and addressable",
			)
		}
		serviceID := provider.Runtime.GeneratedServices[0]
		hostRoutes = append(hostRoutes, resolutionrecord.HostResolutionRouteRecord{
			EnvironmentID: route.EnvironmentID, DesiredRevisionID: task.ID,
			AppliedRevision: route.Observed.Provider.InputRevision, RouteID: route.Desired.ID,
			ServiceID: serviceID, Hostname: route.Desired.Host,
			IPv4: provider.Runtime.PinnedIPv4,
		})
	}
	currentRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{resolutionrecord.StorageKey}, Revision: revision,
	})
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if currentRead == nil || currentRead.ReadRevision != revision || len(currentRead.Values) != 1 {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindInternal,
			"host-resolution projection read is empty",
		)
	}
	current := currentRead.Values[0]
	var stored *resolutionrecord.HostResolutionProjectionRecord
	if current != nil {
		decoded, decodeErr := resolutionrecord.DecodeHostResolutionProjectionRecord(current.Value)
		if decodeErr != nil {
			return hostResolutionReconciliationChange{}, decodeErr
		}
		stored = &decoded
	}
	if isPlatformDNSResolverTaskAttempt(task) {
		preserveHostResolutionDesiredRevisionIDs(stored, hostRoutes)
	}
	publication, err := prepareHostResolutionProjectionPublication(current, revision, hostRoutes)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if publication.record.InputRevision == 0 {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindInternal,
			"host-resolution projection is invalid",
		)
	}
	conditions = appendHostResolutionCondition(conditions, publication.conditions[0])
	projectionChanged := true
	if stored != nil {
		if stored.InputSHA256 == publication.record.InputSHA256 {
			publication.clear()
			publication.record = *stored
			projectionChanged = false
		}
	}
	change := hostResolutionReconciliationChange{applies: true, conditions: conditions}
	if projectionChanged {
		change.mutations = append(change.mutations, publication.mutations...)
		change.values = append(change.values, publication.values...)
	} else {
		publication.clear()
	}

	resolverAttempt := isPlatformDNSResolverTaskAttempt(task)
	if repository.platformResolverTaskPreparer == nil && !resolverAttempt {
		return change, nil
	}
	resolver, err := repository.platformResolverAtRevision(ctx, revision)
	if err != nil {
		clearHostResolutionReconciliationChange(change)
		return hostResolutionReconciliationChange{}, err
	}
	if resolverAttempt && resolver.Record.Desired.ID != task.Target {
		clearHostResolutionReconciliationChange(change)
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindStateConflict,
			"platform resolver Component changed",
		)
	}
	if override, found := componentOverride[resolver.Record.Desired.ID]; found {
		resolver.Record = override
	}
	active, err := repository.platformResolverActiveAtRevision(ctx, resolver.Record.Desired.ID, revision)
	if err != nil {
		clearHostResolutionReconciliationChange(change)
		return hostResolutionReconciliationChange{}, err
	}
	if resolverAttempt && (active == nil || string(active.Value) != task.ID) {
		clearHostResolutionReconciliationChange(change)
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindStateConflict, "platform resolver active Task ownership changed",
		)
	}
	resolverTask := resolverAttempt
	if active != nil && !resolverTask {
		change.conditions = appendHostResolutionCondition(change.conditions, etcdstore.Condition{
			Key: active.Key, ModRevision: active.ModRevision,
		})
		return change, nil
	}
	if !resolver.Record.Desired.Enabled {
		if resolverTask {
			change.conditions = appendHostResolutionCondition(change.conditions, etcdstore.Condition{
				Key: active.Key, ModRevision: active.ModRevision,
			})
			change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: active.Key})
		}
		return change, nil
	}
	if resolverTask {
		if active == nil {
			clearHostResolutionReconciliationChange(change)
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver active Task is missing",
			)
		}
		input, inputErr := repository.platformResolverTaskInputAtRevision(ctx, task, revision)
		if inputErr != nil {
			clearHostResolutionReconciliationChange(change)
			return hostResolutionReconciliationChange{}, inputErr
		}
		stale := stored == nil || stored.InputRevision != input.HostResolutionInputRevision ||
			stored.InputSHA256 != input.HostResolutionSHA256
		if !stale {
			change.conditions = appendHostResolutionCondition(change.conditions, etcdstore.Condition{
				Key: active.Key, ModRevision: active.ModRevision,
			})
			change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: active.Key})
			return change, nil
		}
		successor := newPlatformDNSResolverTask(
			resolver.Record.Desired.ID,
			task.UpdatedAt.Add(time.Nanosecond),
		)
		contribution, contributionErr := repository.preparePlatformDNSResolverTaskContribution(
			ctx, resolver, publication.record, successor, active, &task,
			platformChange.observation, change.conditions,
		)
		if contributionErr != nil {
			clearHostResolutionReconciliationChange(change)
			return hostResolutionReconciliationChange{}, contributionErr
		}
		if !contribution.applies {
			change.conditions = appendHostResolutionCondition(change.conditions, etcdstore.Condition{
				Key: active.Key, ModRevision: active.ModRevision,
			})
			change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: active.Key})
			return change, nil
		}
		change.conditions = contribution.conditions
		change.mutations = append(change.mutations, contribution.mutations...)
		change.values = append(change.values, contribution.values...)
		return change, nil
	}
	if repository.platformResolverTaskPreparer == nil {
		return change, nil
	}
	successor := newPlatformDNSResolverTask(resolver.Record.Desired.ID, time.Now().UTC())
	contribution, contributionErr := repository.preparePlatformDNSResolverTaskContribution(
		ctx, resolver, publication.record, successor, nil, nil, nil, change.conditions,
	)
	if contributionErr != nil {
		clearHostResolutionReconciliationChange(change)
		return hostResolutionReconciliationChange{}, contributionErr
	}
	change.conditions = contribution.conditions
	change.mutations = append(change.mutations, contribution.mutations...)
	change.values = append(change.values, contribution.values...)
	return change, nil
}

func preserveHostResolutionDesiredRevisionIDs(
	current *resolutionrecord.HostResolutionProjectionRecord,
	routes []resolutionrecord.HostResolutionRouteRecord,
) {
	if current == nil {
		return
	}
	previous := make(map[string]resolutionrecord.HostResolutionRouteRecord, len(current.Routes))
	for _, route := range current.Routes {
		previous[route.EnvironmentID+"\x00"+route.RouteID] = route
	}
	for index := range routes {
		prior, found := previous[routes[index].EnvironmentID+"\x00"+routes[index].RouteID]
		if !found || prior.AppliedRevision != routes[index].AppliedRevision ||
			prior.ServiceID != routes[index].ServiceID || prior.Hostname != routes[index].Hostname ||
			prior.IPv4 != routes[index].IPv4 {
			continue
		}
		routes[index].DesiredRevisionID = prior.DesiredRevisionID
	}
}
