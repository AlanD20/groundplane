package attachplan

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ProvisionFacts interface {
	ResolveCapturedProvisionIdentity(context.Context, blueprintattachinputs.Generation, IdentityConsumer) error
	ResolveCapturedProvisionHook(context.Context, blueprintattachinputs.Generation, workexecution.Scope, HookInputConsumer) error
}

type ProvisionPlanner struct{ facts ProvisionFacts }

func NewProvisionPlanner(facts ProvisionFacts) (*ProvisionPlanner, error) {
	if facts == nil {
		return nil, errs.New(errs.KindInternal, "Attach provisioning requires captured fact resolution")
	}
	return &ProvisionPlanner{facts: facts}, nil
}

// BuildCaptured opens only the generation already bound to this work item.
// No mutable topology lookup, generic plan or hidden child Task is involved.
func (planner *ProvisionPlanner) BuildCaptured(ctx context.Context, generation blueprintattachinputs.Generation,
	task workexecution.Task, item workexecution.Item,
) (_ *agentpb.AttachProvisionInput, resultErr error) {
	if planner == nil || planner.facts == nil || ctx == nil || blueprintattachinputs.Validate(generation) != nil ||
		generation.SettlesInController() || !generation.MatchesWork(task, item) ||
		item.TaskID != task.ID || item.ID != generation.WorkItemID || item.Operation != workexecution.Attach ||
		item.TargetID != generation.AttachID || task.Scope.EnvironmentID != generation.EnvironmentID ||
		item.Placement.Executor != workexecution.Agent || item.Input == nil {
		return nil, errs.New(errs.KindStateConflict, "Attach provisioning does not match its captured work")
	}
	reference, err := blueprintattachinputs.WorkInput(generation)
	if err != nil {
		return nil, err
	}
	if *item.Input != reference {
		return nil, errs.New(errs.KindStateConflict, "Attach provisioning generation differs from its work input")
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		return nil, err
	}
	input := &agentpb.AttachProvisionInput{CapturedSource: &agentpb.WorkInputReference{InputId: reference.ID, Sha256: digest},
		AttachId: generation.AttachID, BackingServiceId: generation.BackingServiceID,
		TenantId: task.Scope.TenantID, ProjectId: task.Scope.ProjectID, EnvironmentId: generation.EnvironmentID,
		ServiceId: generation.ConsumerServiceID}
	input.Consumer, err = generation.Consumer()
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			attachexecution.ClearProvision(input)
		}
	}()
	if generation.Provision != nil && generation.Provision.RequiresProvisioning {
		err = planner.facts.ResolveCapturedProvisionIdentity(ctx, generation, func(identity Identity) error {
			if identity.Authentication != generation.Authentication || len(identity.Grants) != len(generation.Provision.Candidate.GrantAttachIDs) {
				return errs.New(errs.KindStateConflict, "Attach provisioning credential capture changed")
			}
			credential := &agentpb.AttachCredentialProvision{AdapterKey: generation.AdapterKey, Role: identity.Role,
				Database: identity.Database, Password: append([]byte(nil), identity.Password...)}
			input.Provision = &agentpb.AttachProvisionInput_Credential{Credential: credential}
			switch identity.Authentication {
			case core.BackingAuthenticationUsernamePassword:
				credential.Authentication = agentpb.BackingAuthentication_BACKING_AUTHENTICATION_USERNAME_PASSWORD
			case core.BackingAuthenticationPassword:
				credential.Authentication = agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD
			default:
				return errs.New(errs.KindStateConflict, "Attach provisioning requires explicit credential authentication")
			}
			for index, grant := range identity.Grants {
				if grant.AttachID != generation.Provision.Candidate.GrantAttachIDs[index] {
					return errs.New(errs.KindStateConflict, "Attach provisioning grant capture changed")
				}
				credential.Grants = append(credential.Grants, &agentpb.AttachGrantProvision{AttachId: grant.AttachID, Database: grant.Database})
			}
			return attachexecution.ValidateProvision(input)
		})
	} else if generation.Hook.Attach != nil {
		err = planner.facts.ResolveCapturedProvisionHook(ctx, generation, task.Scope, func(values backinghook.Input) error {
			definition := generation.Hook.Attach
			hook := &agentpb.BackingHookProcedure{Event: agentpb.BackingHookEvent_BACKING_HOOK_EVENT_ATTACH,
				Command: append([]string(nil), definition.Command...), TimeoutSeconds: definition.TimeoutSeconds,
				BackingServiceId: input.BackingServiceId, AttachId: input.AttachId, TenantId: input.TenantId,
				ProjectId: input.ProjectId, EnvironmentId: input.EnvironmentId, ServiceId: input.ServiceId}
			input.Provision = &agentpb.AttachProvisionInput_CustomAttach{CustomAttach: hook}
			for _, value := range values.Values {
				hook.Inputs = append(hook.Inputs, &agentpb.BackingHookValue{Key: value.Key, Value: append([]byte(nil), value.Value...)})
			}
			for _, fact := range generation.Hook.Facts {
				hook.FactSchema = append(hook.FactSchema, &agentpb.BackingHookFactDefinition{Key: fact.Key, Secret: fact.Secret})
			}
			return attachexecution.ValidateProvision(input)
		})
	}
	if err != nil {
		return nil, err
	}
	if err := attachexecution.ValidateProvision(input); err != nil {
		return nil, err
	}
	return input, nil
}
