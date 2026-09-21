// Package attachplanning seals standalone Attach/Detach plans and prepares their
// selected native runtime updates before any Task is published.
package attachplanning

import (
	"context"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Repository interface {
	GetTenant(context.Context, string) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetService(context.Context, string) (etcdstore.Versioned[servicerecord.ServiceRecord], error)
	GetEnvironmentBlueprintRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[etcd.EnvironmentBlueprintRevision], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	GetEnvironmentComposeProjectionRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	GetAttach(context.Context, string) (etcdstore.Versioned[attachrecord.Record], error)
	GetAttachTaskRenderInput(context.Context, string) (etcdstore.Versioned[etcd.AttachTaskRenderInput], error)
}

type Facts interface {
	ResolveTaskIdentity(
		context.Context,
		etcdstore.Versioned[attachrecord.Record],
		string,
		taskplanning.AttachPlanIdentityConsumer,
	) error
	ResolveHookInput(
		context.Context,
		etcdstore.Versioned[attachrecord.Record],
		etcd.TaskRecord,
		backinghook.Context,
		taskplanning.BackingHookInputConsumer,
	) error
	ResolveDraftHookInput(
		context.Context,
		etcdstore.Versioned[attachrecord.Record],
		*attachrecord.EncryptedFacts,
		etcd.TaskRecord,
		*etcd.BackingHookEncryptedInputs,
		backinghook.Context,
		taskplanning.BackingHookInputConsumer,
	) error
}

type Sealer struct {
	runtimes         *etcd.AttachRepository
	volumeRoot       string
	repository       Repository
	facts            Facts
	componentCatalog []componentrender.EnvironmentComponentRegistration
}

func New(
	volumeRoot string,
	runtimes *etcd.AttachRepository,
	repository Repository,
	facts Facts,
	componentCatalog []componentrender.EnvironmentComponentRegistration,
) (*Sealer, error) {
	if repository == nil || facts == nil || runtimes == nil {
		return nil, errs.New(errs.KindInternal, "Attach draft plan dependencies are required")
	}
	if _, err := taskplanning.NewTaskPlanResolver(volumeRoot, componentCatalog); err != nil {
		return nil, err
	}
	return &Sealer{
		volumeRoot: volumeRoot, repository: repository, facts: facts, runtimes: runtimes,
		componentCatalog: componentrender.CloneEnvironmentComponentCatalog(componentCatalog),
	}, nil
}

func (sealer *Sealer) SealDraft(
	ctx context.Context,
	current etcdstore.Versioned[attachrecord.Record],
	renderInput etcd.AttachTaskRenderInput,
	task etcd.TaskRecord,
	identity *taskplanning.AttachPlanIdentity,
	hookBundle *attachrecord.EncryptedFacts,
	hookInputs *etcd.BackingHookEncryptedInputs,
) (serviceruntimerecord.AttachPreparation, error) {
	state := &draftAttachPlanState{
		repository: sealer.repository, facts: sealer.facts, current: current,
		renderInput: renderInput, identity: identity, hookBundle: hookBundle, hookInputs: hookInputs,
	}
	resolver, err := taskplanning.NewTaskPlanResolverWithAttachments(
		sealer.volumeRoot, sealer.repository, state, sealer.repository, state,
		sealer.componentCatalog,
	)
	if err != nil {
		return serviceruntimerecord.AttachPreparation{}, err
	}
	plan, err := resolver.ResolveExecutionPlan(ctx, task)
	if err != nil {
		return serviceruntimerecord.AttachPreparation{}, err
	}
	defer clearAttachPlanSecrets(plan)
	return sealer.runtimes.PrepareAttachRuntime(ctx, plan)
}

type draftAttachPlanState struct {
	repository  Repository
	facts       Facts
	current     etcdstore.Versioned[attachrecord.Record]
	renderInput etcd.AttachTaskRenderInput
	identity    *taskplanning.AttachPlanIdentity
	hookBundle  *attachrecord.EncryptedFacts
	hookInputs  *etcd.BackingHookEncryptedInputs
}

func (state *draftAttachPlanState) ResolveHookInput(
	ctx context.Context,
	current etcdstore.Versioned[attachrecord.Record],
	task etcd.TaskRecord,
	hookContext backinghook.Context,
	consume taskplanning.BackingHookInputConsumer,
) error {
	if current.Record.ID == state.current.Record.ID && (state.hookBundle != nil || state.hookInputs != nil) {
		return state.facts.ResolveDraftHookInput(
			ctx, current, state.hookBundle, task, state.hookInputs, hookContext, consume,
		)
	}
	return state.facts.ResolveHookInput(ctx, current, task, hookContext, consume)
}

func (state *draftAttachPlanState) GetAttach(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[attachrecord.Record], error) {
	if id == state.current.Record.ID {
		return state.current, nil
	}
	return state.repository.GetAttach(ctx, id)
}

func (state *draftAttachPlanState) GetAttachTaskRenderInput(
	ctx context.Context,
	planID string,
) (etcdstore.Versioned[etcd.AttachTaskRenderInput], error) {
	if planID == state.renderInput.PlanID {
		return etcdstore.Versioned[etcd.AttachTaskRenderInput]{Record: state.renderInput}, nil
	}
	return state.repository.GetAttachTaskRenderInput(ctx, planID)
}

func (state *draftAttachPlanState) GetBlueprintAttachTaskIntent(
	context.Context,
	string,
) (etcdstore.Versioned[etcd.BlueprintAttachTaskIntent], bool, error) {
	return etcdstore.Versioned[etcd.BlueprintAttachTaskIntent]{}, false, nil
}

func (state *draftAttachPlanState) ResolveTaskIdentity(
	ctx context.Context,
	current etcdstore.Versioned[attachrecord.Record],
	taskID string,
	consume taskplanning.AttachPlanIdentityConsumer,
) error {
	if current.Record.ID != state.current.Record.ID || state.identity == nil {
		return state.facts.ResolveTaskIdentity(ctx, current, taskID, consume)
	}
	identity := taskplanning.AttachPlanIdentity{
		Authentication: state.identity.Authentication, Database: state.identity.Database, Role: state.identity.Role,
		Password: append([]byte(nil), state.identity.Password...),
		Grants:   append([]taskplanning.AttachPlanGrantIdentity(nil), state.identity.Grants...),
	}
	defer identity.Clear()
	return consume(identity)
}

func clearAttachPlanSecrets(plan *agentpb.ExecutionPlan) {
	if plan == nil {
		return
	}
	for _, step := range plan.Steps {
		if procedure := step.GetBackingHookProcedure(); procedure != nil {
			taskplan.ClearBackingHookProcedure(procedure)
		}
		procedure := step.GetAdapterProcedure()
		if procedure == nil {
			continue
		}
		clear(procedure.Password)
		procedure.Password = nil
	}
}
