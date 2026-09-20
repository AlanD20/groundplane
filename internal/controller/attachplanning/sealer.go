// Package attachplanning seals standalone Attach/Detach plans and prepares their
// selected native runtime updates before any Task is published.
package attachplanning

import (
	"context"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Repository interface {
	GetTenant(context.Context, string) (etcd.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcd.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
	GetEnvironmentBlueprintRevision(
		context.Context,
		string,
		string,
	) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	GetEnvironmentComposeProjectionRevision(
		context.Context,
		string,
		string,
	) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	GetAttach(context.Context, string) (etcd.Versioned[attachrecord.Record], error)
	GetAttachTaskRenderInput(context.Context, string) (etcd.Versioned[etcd.AttachTaskRenderInput], error)
}

type Facts interface {
	ResolveTaskIdentity(
		context.Context,
		etcd.Versioned[attachrecord.Record],
		string,
		controllerpkg.AttachPlanIdentityConsumer,
	) error
	ResolveHookInput(
		context.Context,
		etcd.Versioned[attachrecord.Record],
		etcd.TaskRecord,
		backinghook.Context,
		controllerpkg.BackingHookInputConsumer,
	) error
	ResolveDraftHookInput(
		context.Context,
		etcd.Versioned[attachrecord.Record],
		*attachrecord.EncryptedFacts,
		etcd.TaskRecord,
		*etcd.BackingHookEncryptedInputs,
		backinghook.Context,
		controllerpkg.BackingHookInputConsumer,
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
	if _, err := controllerpkg.NewTaskPlanResolver(volumeRoot, componentCatalog); err != nil {
		return nil, err
	}
	return &Sealer{
		volumeRoot: volumeRoot, repository: repository, facts: facts, runtimes: runtimes,
		componentCatalog: componentrender.CloneEnvironmentComponentCatalog(componentCatalog),
	}, nil
}

func (sealer *Sealer) SealDraft(
	ctx context.Context,
	current etcd.Versioned[attachrecord.Record],
	renderInput etcd.AttachTaskRenderInput,
	task etcd.TaskRecord,
	identity *controllerpkg.AttachPlanIdentity,
	hookBundle *attachrecord.EncryptedFacts,
	hookInputs *etcd.BackingHookEncryptedInputs,
) (serviceruntimerecord.AttachPreparation, error) {
	state := &draftAttachPlanState{
		repository: sealer.repository, facts: sealer.facts, current: current,
		renderInput: renderInput, identity: identity, hookBundle: hookBundle, hookInputs: hookInputs,
	}
	resolver, err := controllerpkg.NewTaskPlanResolverWithAttachments(
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
	current     etcd.Versioned[attachrecord.Record]
	renderInput etcd.AttachTaskRenderInput
	identity    *controllerpkg.AttachPlanIdentity
	hookBundle  *attachrecord.EncryptedFacts
	hookInputs  *etcd.BackingHookEncryptedInputs
}

func (state *draftAttachPlanState) ResolveHookInput(
	ctx context.Context,
	current etcd.Versioned[attachrecord.Record],
	task etcd.TaskRecord,
	hookContext backinghook.Context,
	consume controllerpkg.BackingHookInputConsumer,
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
) (etcd.Versioned[attachrecord.Record], error) {
	if id == state.current.Record.ID {
		return state.current, nil
	}
	return state.repository.GetAttach(ctx, id)
}

func (state *draftAttachPlanState) GetAttachTaskRenderInput(
	ctx context.Context,
	planID string,
) (etcd.Versioned[etcd.AttachTaskRenderInput], error) {
	if planID == state.renderInput.PlanID {
		return etcd.Versioned[etcd.AttachTaskRenderInput]{Record: state.renderInput}, nil
	}
	return state.repository.GetAttachTaskRenderInput(ctx, planID)
}

func (state *draftAttachPlanState) GetBlueprintAttachTaskIntent(
	context.Context,
	string,
) (etcd.Versioned[etcd.BlueprintAttachTaskIntent], bool, error) {
	return etcd.Versioned[etcd.BlueprintAttachTaskIntent]{}, false, nil
}

func (state *draftAttachPlanState) ResolveTaskIdentity(
	ctx context.Context,
	current etcd.Versioned[attachrecord.Record],
	taskID string,
	consume controllerpkg.AttachPlanIdentityConsumer,
) error {
	if current.Record.ID != state.current.Record.ID || state.identity == nil {
		return state.facts.ResolveTaskIdentity(ctx, current, taskID, consume)
	}
	identity := controllerpkg.AttachPlanIdentity{
		Authentication: state.identity.Authentication, Database: state.identity.Database, Role: state.identity.Role,
		Password: append([]byte(nil), state.identity.Password...),
		Grants:   append([]controllerpkg.AttachPlanGrantIdentity(nil), state.identity.Grants...),
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
