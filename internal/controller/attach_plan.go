package controller

import (
	"context"
	"math"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type AttachPlanGrantIdentity struct {
	AttachID string
	Database string
}

type AttachPlanIdentity struct {
	Database string
	Role     string
	Password []byte
	Grants   []AttachPlanGrantIdentity
}

func (identity *AttachPlanIdentity) Clear() {
	if identity == nil {
		return
	}
	clear(identity.Password)
	identity.Password = nil
	identity.Grants = nil
}

type AttachPlanIdentityConsumer func(AttachPlanIdentity) error

type attachPlanRecordReader interface {
	GetAttach(context.Context, string) (etcd.Versioned[etcd.AttachRecord], error)
}

type attachPlanServiceReader interface {
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
}

type attachPlanIdentityResolver interface {
	ResolveTaskIdentity(
		context.Context,
		etcd.Versioned[etcd.AttachRecord],
		string,
		AttachPlanIdentityConsumer,
	) error
}

func NewTaskPlanResolverWithAttachments(
	volumeRoot string,
	blueprints blueprintPlanStateReader,
	attaches attachPlanRecordReader,
	services attachPlanServiceReader,
	identities attachPlanIdentityResolver,
) (*TaskPlanResolver, error) {
	resolver, err := NewTaskPlanResolverWithBlueprints(volumeRoot, blueprints)
	if err != nil {
		return nil, err
	}
	if attaches == nil || services == nil || identities == nil {
		return nil, errs.New(errs.KindInternal, "Attach plan dependencies are required")
	}
	resolver.attaches = attaches
	resolver.services = services
	resolver.attachIdentities = identities
	return resolver, nil
}

func (resolver *TaskPlanResolver) resolveAttachPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if resolver.attaches == nil || resolver.services == nil || resolver.attachIdentities == nil ||
		task.Executor != etcd.TaskExecutorAgent || ids.Validate(ids.KindAttach, task.Target) != nil ||
		len(task.Params) != 0 || len(task.Materializations) != 0 || task.TimeoutSeconds <= 0 ||
		task.TimeoutSeconds > math.MaxUint32 {
		return nil, errs.New(errs.KindInternal, "durable Attach Task shape is invalid")
	}
	current, err := resolver.attaches.GetAttach(ctx, task.Target)
	if err != nil {
		return nil, err
	}
	if current.Record.TaskID != task.ID {
		return nil, errs.New(errs.KindInternal, "durable Attach Task no longer owns its target")
	}
	operation, err := attachPlanOperation(task, current.Record)
	if err != nil {
		return nil, err
	}
	backing, err := resolver.services.GetService(ctx, current.Record.BackingServiceID)
	if err != nil {
		return nil, err
	}
	if backing.Record.EnvironmentID != current.Record.BackingEnvironmentID ||
		backing.Record.Desired.ID != current.Record.BackingServiceID ||
		backing.Record.Runtime.ServiceID != current.Record.BackingServiceID ||
		backing.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		return nil, errs.New(errs.KindStateConflict, "Attach backing Service is not runnable")
	}
	adapter, found := adapters.Get(backing.Record.Desired.Adapter)
	if !found {
		return nil, errs.New(errs.KindValidationFailed, "Attach backing Service adapter is not registered")
	}
	if adapter.Manual() {
		return nil, errs.New(errs.KindNotImplemented, "manual Attach network procedure is not implemented")
	}
	if err := resolver.validateAttachGrantTargets(ctx, current.Record); err != nil {
		return nil, err
	}
	var plan *agentpb.ExecutionPlan
	err = resolver.attachIdentities.ResolveTaskIdentity(ctx, current, task.ID, func(identity AttachPlanIdentity) error {
		steps, buildErr := attachProcedureSteps(task, current.Record, backing.Record.Desired.Adapter, identity)
		if buildErr != nil {
			return buildErr
		}
		defer clearAdapterProcedurePasswords(steps)
		plan, buildErr = BuildPlan(PlanBuildInput{
			VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
			RenderGeneration: uint64(task.RenderGeneration), Operation: operation,
			TargetID: task.Target, Steps: steps,
		})
		return buildErr
	})
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func attachPlanOperation(task etcd.TaskRecord, record etcd.AttachRecord) (agentpb.PlanOperation, error) {
	switch task.Type {
	case etcd.TaskAttach:
		if record.Operation != etcd.AttachOperationProvision ||
			(record.Status != core.AttachPending && record.Status != core.AttachProvisioning) {
			return 0, errs.New(errs.KindStateConflict, "Attach is not provisionable")
		}
		return agentpb.PlanOperation_PLAN_OPERATION_ATTACH, nil
	case etcd.TaskDetach:
		if record.Operation != etcd.AttachOperationDetach || record.Status != core.AttachDetaching {
			return 0, errs.New(errs.KindStateConflict, "Attach is not detaching")
		}
		return agentpb.PlanOperation_PLAN_OPERATION_DETACH, nil
	default:
		return 0, errs.New(errs.KindInternal, "durable Attach Task type is invalid")
	}
}

func (resolver *TaskPlanResolver) validateAttachGrantTargets(ctx context.Context, record etcd.AttachRecord) error {
	for _, grantID := range record.GrantAttachIDs {
		grant, err := resolver.attaches.GetAttach(ctx, grantID)
		if err != nil {
			return err
		}
		if grant.Record.Status != core.AttachReady || grant.Record.BackingServiceID != record.BackingServiceID {
			return errs.New(errs.KindStateConflict, "Attach grant target is not ready on the same backing Service")
		}
	}
	return nil
}

func attachProcedureSteps(
	task etcd.TaskRecord,
	record etcd.AttachRecord,
	adapterKey string,
	identity AttachPlanIdentity,
) ([]*agentpb.ExecutionStep, error) {
	if identity.Role == "" || len(identity.Password) == 0 || len(identity.Grants) != len(record.GrantAttachIDs) ||
		len(task.Steps) != len(record.GrantAttachIDs)+1 {
		return nil, errs.New(errs.KindInternal, "Attach task identity or step count is invalid")
	}
	for index, grant := range identity.Grants {
		if grant.AttachID != record.GrantAttachIDs[index] || grant.Database == "" {
			return nil, errs.New(errs.KindInternal, "Attach task grant identity is invalid")
		}
	}
	steps := make([]*agentpb.ExecutionStep, 0, len(task.Steps))
	appendProcedure := func(index int, phase agentpb.AdapterProcedurePhase, database, grantOn string, password []byte) {
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: task.Steps[index].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_AdapterProcedure{AdapterProcedure: &agentpb.AdapterProcedure{
				AdapterKey: adapterKey, Phase: phase, AttachId: record.ID,
				BackingServiceId: record.BackingServiceID, Role: identity.Role,
				Password: append([]byte(nil), password...), Database: database, GrantOn: grantOn,
			}},
		})
	}
	if task.Type == etcd.TaskAttach {
		appendProcedure(0, agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION,
			identity.Database, "", identity.Password)
		for index, grant := range identity.Grants {
			appendProcedure(index+1, agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_GRANT,
				"", grant.Database, nil)
		}
		return steps, nil
	}
	for index := len(identity.Grants) - 1; index >= 0; index-- {
		grant := identity.Grants[index]
		stepIndex := len(identity.Grants) - 1 - index
		appendProcedure(stepIndex, agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_REVOKE,
			"", grant.Database, nil)
	}
	appendProcedure(len(identity.Grants), agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH,
		identity.Database, "", nil)
	return steps, nil
}

func clearAdapterProcedurePasswords(steps []*agentpb.ExecutionStep) {
	for _, step := range steps {
		procedure := step.GetAdapterProcedure()
		if procedure == nil {
			continue
		}
		clear(procedure.Password)
		procedure.Password = nil
	}
}
