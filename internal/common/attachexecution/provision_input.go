package attachexecution

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/adapterprocedure"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ValidateProvision admits a captured credential or Attach hook, never an
// arbitrary sequence of adapter commands. Bindings without provisioning are
// settled by the Controller and do not manufacture an Agent no-op.
func ValidateProvision(input *agentpb.AttachProvisionInput) error {
	if input == nil || executionplan.RejectUnknown(input) != nil || proto.Size(input) > executionplan.MaximumPlanBytes ||
		input.CapturedSource == nil || ids.Validate(ids.KindPlan, input.CapturedSource.InputId) != nil || len(input.CapturedSource.Sha256) != 32 ||
		ids.Validate(ids.KindAttach, input.AttachId) != nil || ids.Validate(ids.KindService, input.BackingServiceId) != nil ||
		ids.Validate(ids.KindTenant, input.TenantId) != nil || ids.Validate(ids.KindProject, input.ProjectId) != nil ||
		ids.Validate(ids.KindEnvironment, input.EnvironmentId) != nil || input.ServiceId != "" && ids.Validate(ids.KindService, input.ServiceId) != nil {
		return invalid("Attach provisioning identity or captured source is invalid")
	}
	if input.Consumer != nil {
		if input.Consumer.Action != agentpb.AttachNetworkAction_ATTACH_NETWORK_ACTION_CONNECT || input.Consumer.EnvironmentId != input.EnvironmentId || input.Consumer.ServiceId != input.ServiceId {
			return invalid("Attach consumer connection belongs to a different binding")
		}
		if err := executionplan.ValidateAttachConsumerTransition(context.Background(), input.Consumer); err != nil {
			return err
		}
	}
	switch selected := input.Provision.(type) {
	case nil:
		if input.Consumer == nil {
			return invalid("Attach requires provisioning or a consumer connection")
		}
		return nil
	case *agentpb.AttachProvisionInput_Credential:
		if selected.Credential == nil || len(selected.Credential.Grants) > MaximumGrants {
			return invalid("Attach credential provisioning is missing or oversized")
		}
		if err := adapterprocedure.Validate(provisionProcedure(input, nil)); err != nil {
			return err
		}
		seenIDs := make(map[string]bool)
		for _, grant := range selected.Credential.Grants {
			if grant == nil || ids.Validate(ids.KindAttach, grant.AttachId) != nil || grant.AttachId == input.AttachId || seenIDs[grant.AttachId] {
				return invalid("Attach provisioning grant identity is repeated or invalid")
			}
			seenIDs[grant.AttachId] = true
			if err := adapterprocedure.Validate(provisionProcedure(input, grant)); err != nil {
				return err
			}
		}
		return nil
	case *agentpb.AttachProvisionInput_CustomAttach:
		hook := selected.CustomAttach
		if hook == nil || hook.Event != agentpb.BackingHookEvent_BACKING_HOOK_EVENT_ATTACH ||
			hook.AttachId != input.AttachId || hook.BackingServiceId != input.BackingServiceId || hook.TenantId != input.TenantId ||
			hook.ProjectId != input.ProjectId || hook.EnvironmentId != input.EnvironmentId || hook.ServiceId != input.ServiceId || len(hook.Facts) != 0 {
			return invalid("Custom Attach hook differs from its captured binding")
		}
		definition, values, schema, err := executionplan.DecodeBackingHookProcedure(hook)
		if err != nil {
			return err
		}
		return backinghook.Validate(definition, values, schema)
	default:
		return invalid("Attach provisioning requires a credential or Custom Attach hook")
	}
}

// ProvisionProcedures preserves captured grant order after credential creation.
// Password bytes in the returned procedures belong to the caller.
func ProvisionProcedures(input *agentpb.AttachProvisionInput) ([]*agentpb.AdapterProcedure, error) {
	if err := ValidateProvision(input); err != nil {
		return nil, err
	}
	credential := input.GetCredential()
	if credential == nil {
		return nil, invalid("Custom Attach has no built-in credential procedures")
	}
	procedures := make([]*agentpb.AdapterProcedure, 0, len(credential.Grants)+1)
	procedures = append(procedures, provisionProcedure(input, nil))
	for _, grant := range credential.Grants {
		procedures = append(procedures, provisionProcedure(input, grant))
	}
	procedures[0].Password = append([]byte(nil), credential.Password...)
	return procedures, nil
}

func provisionProcedure(input *agentpb.AttachProvisionInput, grant *agentpb.AttachGrantProvision) *agentpb.AdapterProcedure {
	credential := input.GetCredential()
	procedure := &agentpb.AdapterProcedure{AdapterKey: credential.AdapterKey, Authentication: credential.Authentication,
		AttachId: input.AttachId, BackingServiceId: input.BackingServiceId, Role: credential.Role}
	if grant == nil {
		procedure.Phase = agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION
		procedure.Database, procedure.Password = credential.Database, credential.Password
	} else {
		procedure.Phase, procedure.GrantOn = agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_GRANT, grant.Database
	}
	return procedure
}

func ClearProvision(input *agentpb.AttachProvisionInput) {
	if input == nil {
		return
	}
	if input.Consumer != nil {
		for _, artifact := range []*agentpb.ComposeArtifact{input.Consumer.Baseline, input.Consumer.Candidate} {
			if artifact != nil {
				clear(artifact.CanonicalYaml)
			}
		}
	}
	if credential := input.GetCredential(); credential != nil {
		clear(credential.Password)
		credential.Password = nil
	}
	if hook := input.GetCustomAttach(); hook != nil {
		for _, values := range [][]*agentpb.BackingHookValue{hook.Inputs, hook.Facts} {
			for _, value := range values {
				if value != nil {
					clear(value.Value)
					value.Value = nil
				}
			}
		}
	}
}
