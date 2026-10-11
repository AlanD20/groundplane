package blueprint

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type authoredAttachRemovalFactReader interface {
	GetAttachFactsVersioned(
		context.Context,
		etcdstore.Versioned[attachrecord.Record],
	) (etcdstore.Versioned[attachrecord.EncryptedFacts], bool, error)
}

func (service *Service) prepareAuthoredAttachRemovalChild(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
	timeoutSeconds int64,
) (blueprintcoordinator.PreparedChild, error) {
	if !unit.Removal || unit.Target.Kind != ids.KindAttach || service.plans == nil || service.attachFacts == nil {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindValidationFailed, "Blueprint Attach removal child identity is invalid",
		)
	}
	applied, found, err := service.repository.GetEnvironmentAppliedComposeProjection(
		ctx, parent.Owner.EnvironmentID,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if !found {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Attach applied projection is unavailable",
		)
	}
	head, found, err := service.repository.GetEnvironmentBlueprintHead(ctx, parent.Owner.EnvironmentID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if !found || head.Record.RevisionID != parent.ID || head.Revision <= 0 {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Attach removal parent is no longer current",
		)
	}
	topology, source, err := service.loadAuthoredAttachRemovalTopology(
		ctx, parent.Owner.EnvironmentID, unit.Target.ID, applied.ReadRevision,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	createdAt := service.now().UTC()
	child := newAuthoredAttachRemovalTask(parent, source.Record, timeoutSeconds, createdAt)
	if applied.Record.RenderGeneration == 0 || applied.Record.RenderGeneration > math.MaxInt32 {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Attach applied render generation is invalid",
		)
	}
	child.RenderGeneration = int32(applied.Record.RenderGeneration)
	detaching, err := authoredAttachRemovalTransition(source.Record, child.ID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	factReader, ok := service.repository.(authoredAttachRemovalFactReader)
	if !ok {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindInternal, "Blueprint Attach removal fact reader is not configured",
		)
	}
	facts, factsFound, err := factReader.GetAttachFactsVersioned(ctx, source)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if factsFound {
		defer clear(facts.Record.Ciphertext)
	}
	scope, err := service.loadAuthoredAttachRemovalScope(ctx, parent, source.Record, applied)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	adapter, found := adapters.Get(scope.BackingService.Record.Desired.Adapter)
	if !found {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Attach backing adapter is unavailable",
		)
	}
	authentication, err := core.ResolveBackingAuthentication(
		adapter.SupportsAuthenticationModes(), scope.BackingService.Record.Desired.Authentication,
	)
	if err != nil || authentication != scope.BackingService.Record.Desired.Authentication {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Attach backing authentication changed",
		)
	}
	detachHook := detaching.HookBundle && detaching.OwnsCredential() &&
		scope.BackingService.Record.Desired.Hooks != nil &&
		scope.BackingService.Record.Desired.Hooks.Detach != nil
	if detachHook {
		requiredTimeoutSeconds := int64(scope.BackingService.Record.Desired.Hooks.Detach.TimeoutSeconds) + 15
		if requiredTimeoutSeconds > child.TimeoutSeconds {
			return blueprintcoordinator.PreparedChild{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Attach detach hook exceeds the remaining parent timeout",
			)
		}
	}
	var hookInputs *taskconfiguration.BackingHookEncryptedInputs
	if detachHook {
		hookInputs, err = service.attachFacts.SealBackingHookTaskInputs(
			ctx, child.OperationID, scope.BackingProject.Record.ID,
			*scope.BackingService.Record.Desired.Hooks,
		)
		if err != nil {
			return blueprintcoordinator.PreparedChild{}, err
		}
		if hookInputs != nil {
			defer clear(hookInputs.Ciphertext)
			child, err = etcd.BindBackingHookTaskInputs(
				child, scope.BackingProject.Record.ID, *hookInputs,
			)
			if err != nil {
				return blueprintcoordinator.PreparedChild{}, err
			}
		}
	}
	runtime, err := service.plans.CaptureEntryMutationRuntime(ctx, applied)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	renderInput, err := attachments.BuildAuthoredDetachTaskRenderInput(
		scope, runtime, detaching, authoredAttachRemovalRecords(topology), child, ids.New(ids.KindConfig),
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	desired := servicerecord.ServiceDesiredCondition(scope.BackingService)
	intent := environmentchanges.AttachRemovalIntent{
		TaskID: child.ID, ParentTaskID: parent.ID, EnvironmentID: parent.Owner.EnvironmentID,
		AttachID: unit.Target.ID, ExpectedHeadRevision: head.Revision,
		AppliedProjectionRevision: applied.Revision, SourceRevision: source.Revision,
		FactsRevision: facts.Revision, TenantRevision: scope.Tenant.Revision,
		ProjectRevision: scope.Project.Revision, EnvironmentRevision: scope.Environment.Revision,
		BackingProjectRevision:     scope.BackingProject.Revision,
		BackingEnvironmentRevision: scope.BackingEnvironment.Revision,
		BackingDesiredKey:          desired.Key, BackingDesiredRevision: desired.ModRevision,
		BackingRuntimeRevision: servicerecord.ServiceRuntimeRevision(scope.BackingService),
		Source:                 source.Record, Detaching: detaching, Topology: topology,
		AppliedProjection: applied.Record, RenderInput: renderInput,
		Status: taskjournal.TaskStatusPending, CreatedAt: createdAt,
	}
	if factsFound {
		copy := facts.Record
		copy.Ciphertext = append([]byte(nil), facts.Record.Ciphertext...)
		intent.Facts = &copy
	}
	defer environmentchanges.ClearAttachRemovalIntent(&intent)
	prepared, plan, err := taskplanning.PrepareBlueprintAttachRemovalTask(
		ctx, service.volumeRoot, service.attachFacts, intent, child, hookInputs,
	)
	if err != nil {
		clearAuthoredAttachRemovalPlan(plan)
		return blueprintcoordinator.PreparedChild{}, err
	}
	defer clearAuthoredAttachRemovalPlan(plan)
	runtimeSources, err := authoredAttachRemovalRuntimeSources(runtime, source.Record.ServiceID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	preparation, err := etcd.PrepareAuthoredAttachRuntime(plan, runtimeSources)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	prepared.TargetName = source.Record.Name
	intent.RenderInput.RuntimePreparation = &preparation
	publication, err := etcd.NewBlueprintAttachRemovalPublication(prepared, intent, hookInputs)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	return blueprintcoordinator.PreparedChild{
		Task: prepared, AttachRemovalPublication: &publication,
	}, nil
}

func authoredAttachRemovalRuntimeSources(
	runtime taskplanning.EntryMutationRuntime,
	serviceID string,
) ([]etcdstore.Versioned[serviceruntimerecord.Record], error) {
	running := false
	for _, candidate := range runtime.RunningServiceIDs {
		if candidate == serviceID {
			running = true
			break
		}
	}
	var selected []etcdstore.Versioned[serviceruntimerecord.Record]
	for _, source := range runtime.Sources {
		if source.Record.Runtime.ServiceID == serviceID {
			selected = append(selected, source)
		}
	}
	if len(selected) > 1 || running != (len(selected) == 1) {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Attach acknowledged runtime changed")
	}
	return selected, nil
}

func newAuthoredAttachRemovalTask(
	parent etcd.TaskRecord,
	source attachrecord.Record,
	timeoutSeconds int64,
	createdAt time.Time,
) etcd.TaskRecord {
	return etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), PlanID: ids.New(ids.KindPlan),
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem, Executor: taskjournal.TaskExecutorAgent,
		Type: taskjournal.TaskDetach, Target: source.ID,
		RenderGeneration: int32(parent.RenderGeneration),
		Params: map[string]string{
			blueprints.EnvironmentDesiredRevisionParam: parent.ID,
			taskjournal.TaskBlueprintParentParam:       parent.ID,
			attachinputs.TaskAttachIDParam:             source.ID,
			taskjournal.TaskMutationEnvironmentParam:   source.EnvironmentID,
		},
		TimeoutSeconds: timeoutSeconds, Status: taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
}

func authoredAttachRemovalTransition(source attachrecord.Record, taskID string) (attachrecord.Record, error) {
	if source.Status == core.AttachFailed && source.Operation == attachrecord.AttachOperationDetach {
		return attachrecord.RetryAttachOperation(source, taskID)
	}
	return attachrecord.BeginAttachDetaching(source, taskID)
}

func authoredAttachRemovalRecords(
	topology []environmentchanges.AttachRemovalSource,
) []etcdstore.Versioned[attachrecord.Record] {
	result := make([]etcdstore.Versioned[attachrecord.Record], len(topology))
	for index, source := range topology {
		result[index] = etcdstore.Versioned[attachrecord.Record]{
			Record: source.Record, Revision: source.Revision, ReadRevision: source.Revision,
		}
	}
	return result
}

func clearAuthoredAttachRemovalPlan(plan *agentpb.ExecutionPlan) {
	if plan == nil {
		return
	}
	for _, step := range plan.GetSteps() {
		taskplan.ClearBackingHookProcedure(step.GetBackingHookProcedure())
		if procedure := step.GetAdapterProcedure(); procedure != nil {
			clear(procedure.Password)
			procedure.Password = nil
		}
	}
}

func (service *Service) loadAuthoredAttachRemovalTopology(
	ctx context.Context,
	environmentID string,
	attachID string,
	revision int64,
) ([]environmentchanges.AttachRemovalSource, etcdstore.Versioned[attachrecord.Record], error) {
	var topology []environmentchanges.AttachRemovalSource
	var selected etcdstore.Versioned[attachrecord.Record]
	cursor := ""
	for {
		page, err := service.repository.ListAttaches(ctx, environmentID, etcdstore.PageRequest{
			Limit: etcdstore.MaximumPageLimit, Cursor: cursor, Revision: revision,
		})
		if err != nil {
			return nil, etcdstore.Versioned[attachrecord.Record]{}, err
		}
		if page.Revision != revision {
			return nil, etcdstore.Versioned[attachrecord.Record]{}, errs.New(
				errs.KindStateConflict, "Blueprint Attach topology read changed",
			)
		}
		for _, item := range page.Items {
			topology = append(topology, environmentchanges.AttachRemovalSource{
				Record: item.Record, Revision: item.Revision,
			})
			if item.Record.ID == attachID {
				selected = item
			}
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if selected.Revision <= 0 {
		return nil, etcdstore.Versioned[attachrecord.Record]{}, errs.New(
			errs.KindStateConflict, "Blueprint Attach removal source is unavailable",
		)
	}
	sort.Slice(topology, func(left, right int) bool {
		return topology[left].Record.ID < topology[right].Record.ID
	})
	return topology, selected, nil
}

func (service *Service) loadAuthoredAttachRemovalScope(
	ctx context.Context,
	parent etcd.TaskRecord,
	source attachrecord.Record,
	applied etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
) (attachments.AuthoredDetachRenderScope, error) {
	environment, err := service.repository.GetEnvironment(ctx, source.EnvironmentID)
	if err != nil {
		return attachments.AuthoredDetachRenderScope{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return attachments.AuthoredDetachRenderScope{}, err
	}
	tenant, err := service.authoredOwnerTenant(ctx, project.Record)
	if err != nil {
		return attachments.AuthoredDetachRenderScope{}, err
	}
	backingProject, err := service.repository.GetProject(ctx, source.BackingProjectID)
	if err != nil {
		return attachments.AuthoredDetachRenderScope{}, err
	}
	backingEnvironment, err := service.repository.GetEnvironment(ctx, source.BackingEnvironmentID)
	if err != nil {
		return attachments.AuthoredDetachRenderScope{}, err
	}
	backingService, err := service.repository.GetService(ctx, source.BackingServiceID)
	if err != nil {
		return attachments.AuthoredDetachRenderScope{}, err
	}
	consumerFound := false
	for _, candidate := range applied.Record.DesiredServices {
		if candidate.Desired.ID == source.ServiceID {
			consumerFound = true
			break
		}
	}
	if parent.Owner.EnvironmentID != source.EnvironmentID ||
		applied.Record.EnvironmentID != source.EnvironmentID ||
		environment.Record.ID != source.EnvironmentID || project.Record.ID != parent.Owner.ProjectID ||
		tenant.Record.ID != parent.Owner.TenantID || !consumerFound ||
		backingProject.Record.ID != source.BackingProjectID ||
		backingEnvironment.Record.ID != source.BackingEnvironmentID ||
		backingEnvironment.Record.ProjectID != backingProject.Record.ID ||
		backingService.Record.Desired.ID != source.BackingServiceID ||
		backingService.Record.EnvironmentID != backingEnvironment.Record.ID ||
		backingService.Record.BackingNetworkID != source.BackingNetworkID ||
		backingService.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		return attachments.AuthoredDetachRenderScope{}, errs.New(
			errs.KindStateConflict, "Blueprint Attach removal hierarchy changed",
		)
	}
	return attachments.AuthoredDetachRenderScope{
		Tenant: tenant, Project: project, Environment: environment, AppliedProjection: applied,
		BackingProject: backingProject, BackingEnvironment: backingEnvironment,
		BackingService: backingService,
	}, nil
}
