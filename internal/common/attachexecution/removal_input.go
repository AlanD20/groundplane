// Package attachexecution owns typed backing credential and hook execution.
package attachexecution

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/adapterprocedure"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const MaximumGrants = 8

type RemovalInput struct {
	Attempt workexecution.Attempt
	Removal *agentpb.AttachRemovalInput
}

func ValidateRemoval(input *agentpb.AttachRemovalInput) error {
	if input == nil || executionplan.RejectUnknown(input) != nil || proto.Size(input) > executionplan.MaximumPlanBytes ||
		input.CapturedSource == nil || ids.Validate(ids.KindPlan, input.CapturedSource.InputId) != nil || len(input.CapturedSource.Sha256) != 32 ||
		ids.Validate(ids.KindAttach, input.AttachId) != nil || ids.Validate(ids.KindService, input.BackingServiceId) != nil ||
		ids.Validate(ids.KindTenant, input.TenantId) != nil || ids.Validate(ids.KindProject, input.ProjectId) != nil ||
		ids.Validate(ids.KindEnvironment, input.EnvironmentId) != nil || input.ServiceId != "" && ids.Validate(ids.KindService, input.ServiceId) != nil {
		return invalid("Attach removal identity or captured source is invalid")
	}
	if input.Consumer != nil {
		if input.Consumer.EnvironmentId != input.EnvironmentId || input.Consumer.ServiceId != input.ServiceId || input.Consumer.Action != agentpb.AttachNetworkAction_ATTACH_NETWORK_ACTION_DISCONNECT {
			return invalid("Detach consumer belongs to a different Attach")
		}
		if err := executionplan.ValidateAttachConsumerTransition(context.Background(), input.Consumer); err != nil {
			return err
		}
	}
	switch selected := input.Cleanup.(type) {
	case nil:
		if input.Consumer == nil {
			return invalid("Attach removal has no consumer or credential effects")
		}
		return nil
	case *agentpb.AttachRemovalInput_Credential:
		if selected.Credential == nil || len(selected.Credential.Grants) > MaximumGrants {
			return invalid("Attach credential removal is missing or oversized")
		}
		seen := make(map[string]bool)
		for _, grant := range selected.Credential.Grants {
			if grant == nil || ids.Validate(ids.KindAttach, grant.AttachId) != nil || grant.AttachId == input.AttachId || seen[grant.AttachId] {
				return invalid("Attach grant removal identity is invalid or repeated")
			}
			seen[grant.AttachId] = true
			if err := adapterprocedure.Validate(removalProcedure(input, grant)); err != nil {
				return err
			}
		}
		return adapterprocedure.Validate(removalProcedure(input, nil))
	case *agentpb.AttachRemovalInput_CustomDetach:
		hook := selected.CustomDetach
		if hook == nil || hook.Event != agentpb.BackingHookEvent_BACKING_HOOK_EVENT_DETACH ||
			hook.AttachId != input.AttachId || hook.BackingServiceId != input.BackingServiceId || hook.TenantId != input.TenantId ||
			hook.ProjectId != input.ProjectId || hook.EnvironmentId != input.EnvironmentId || hook.ServiceId != input.ServiceId {
			return invalid("Custom Detach hook belongs to a different captured Attach")
		}
		definition, values, schema, err := executionplan.DecodeBackingHookProcedure(hook)
		if err != nil {
			return err
		}
		return backinghook.Validate(definition, values, schema)
	default:
		return invalid("Attach removal requires a credential or Custom Detach procedure")
	}
}

// CredentialProcedures yields the closed adapter operations in removal order:
// revoke each captured grant in reverse order, then remove the credential.
func CredentialProcedures(input *agentpb.AttachRemovalInput) ([]*agentpb.AdapterProcedure, error) {
	if err := ValidateRemoval(input); err != nil {
		return nil, err
	}
	credential := input.GetCredential()
	if credential == nil {
		return nil, invalid("Custom Detach does not have built-in credential procedures")
	}
	procedures := make([]*agentpb.AdapterProcedure, 0, len(credential.Grants)+1)
	for index := len(credential.Grants) - 1; index >= 0; index-- {
		procedures = append(procedures, removalProcedure(input, credential.Grants[index]))
	}
	procedures = append(procedures, removalProcedure(input, nil))
	return procedures, nil
}

func removalProcedure(input *agentpb.AttachRemovalInput, grant *agentpb.AttachGrantRemoval) *agentpb.AdapterProcedure {
	credential := input.GetCredential()
	procedure := &agentpb.AdapterProcedure{AdapterKey: credential.AdapterKey, Authentication: credential.Authentication,
		AttachId: input.AttachId, BackingServiceId: input.BackingServiceId, Role: credential.Role}
	if grant != nil {
		procedure.Phase, procedure.GrantOn = agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_REVOKE, grant.Database
	} else {
		procedure.Phase, procedure.Database, procedure.Password = agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH, credential.Database, credential.Password
	}
	return procedure
}

func ClearRemoval(input *agentpb.AttachRemovalInput) {
	if input == nil {
		return
	}
	if consumer := input.Consumer; consumer != nil {
		if consumer.Baseline != nil {
			clear(consumer.Baseline.CanonicalYaml)
		}
		if consumer.Candidate != nil {
			clear(consumer.Candidate.CanonicalYaml)
		}
		input.Consumer = nil
	}
	if credential := input.GetCredential(); credential != nil {
		clear(credential.Password)
		credential.Password = nil
	}
	if hook := input.GetCustomDetach(); hook != nil {
		for _, value := range hook.Inputs {
			if value != nil {
				clear(value.Value)
				value.Value = nil
			}
		}
		for _, value := range hook.Facts {
			if value != nil {
				clear(value.Value)
				value.Value = nil
			}
		}
	}
}

func DecodeRemovalAssignment(message *agentpb.AttachRemovalAssignment, identity workexecution.Identity) (RemovalInput, error) {
	if message == nil || executionplan.RejectUnknown(message) != nil {
		return RemovalInput{}, invalid("Attach removal assignment is invalid")
	}
	if err := ValidateRemoval(message.Removal); err != nil {
		return RemovalInput{}, err
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, identity)
	if err != nil || attempt.Direction != workexecution.Forward {
		return RemovalInput{}, invalid("Attach removal requires its captured forward assignment")
	}
	input := RemovalInput{Attempt: attempt, Removal: proto.CloneOf(message.Removal)}
	if err := input.Validate(); err != nil {
		ClearRemoval(input.Removal)
		return RemovalInput{}, err
	}
	return input, nil
}

func EncodeRemovalAssignment(input RemovalInput) (*agentpb.AttachRemovalAssignment, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	if input.Attempt.Direction != workexecution.Forward {
		return nil, invalid("Attach removal requires a forward attempt")
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	return &agentpb.AttachRemovalAssignment{Attempt: attempt, Removal: proto.CloneOf(input.Removal)}, nil
}

func invalid(detail string) error { return errs.New(errs.KindValidationFailed, detail) }
