package executionplan

import (
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ValidateServiceLifecycleWorkInput validates the native procedure directly.
// The Agent must additionally enforce attempt/host identity and inspect actual
// container ownership before honoring any checkpoint grant.
func ValidateServiceLifecycleWorkInput(input *agentpb.ServiceLifecycleWorkInput) error {
	if input == nil || proto.Size(input) > MaximumPlanBytes ||
		ids.Validate(
			ids.KindEnvironment,
			input.EnvironmentId,
		) != nil || ids.Validate(ids.KindService, input.ServiceId) != nil ||
		input.CapturedSource == nil || ids.Validate(ids.KindPlan, input.CapturedSource.InputId) != nil ||
		len(
			input.CapturedSource.Sha256,
		) != sha256.Size || len(input.Members) == 0 || len(input.Members) > MaximumServiceRuntimeMembers {
		return invalidLifecycleWork()
	}
	if err := RejectUnknown(input); err != nil {
		return err
	}
	switch input.Action {
	case agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_START,
		agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_DESTROY:
		if input.StopGraceSeconds != 0 {
			return invalidLifecycleWork()
		}
	case agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_STOP:
		if input.StopGraceSeconds != 30 {
			return invalidLifecycleWork()
		}
	default:
		return invalidLifecycleWork()
	}
	stages, artifacts, names := make(map[string]bool), make(map[string]bool), make(map[string]bool)
	for _, member := range input.Members {
		if member == nil || member.Ownership == nil {
			return invalidLifecycleWork()
		}
		artifact, source := member.Artifact, member.Ownership
		if err := validateArtifactContent(artifact); err != nil {
			return err
		}
		if artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
			artifact.OwnerId != input.EnvironmentId ||
			len(artifact.Services) != 1 ||
			artifact.Services[0] == nil ||
			source.ArtifactId != artifact.ArtifactId ||
			source.ServiceId != input.ServiceId ||
			ids.Validate(ids.KindPlan, source.SourcePlanId) != nil ||
			source.SourceRenderGeneration == 0 ||
			ids.Validate(ids.KindStep, source.StepId) != nil ||
			stages[source.StepId] ||
			artifacts[source.ArtifactId] ||
			len(source.ComposeNames) != 1 {
			return invalidLifecycleWork()
		}
		service := artifact.Services[0]
		if service.ServiceId != input.ServiceId || service.ComposeName == "" ||
			source.ComposeNames[0] != service.ComposeName ||
			names[service.ComposeName] ||
			!serviceLifecycleLabelsMatch(service, source) {
			return invalidLifecycleWork()
		}
		stages[source.StepId], artifacts[source.ArtifactId], names[service.ComposeName] = true, true, true
	}
	if hook := input.Hook; hook != nil {
		if ids.Validate(ids.KindStep, hook.StageId) != nil || stages[hook.StageId] {
			return invalidLifecycleWork()
		}
		definition := &backinghook.Definition{
			Command:        append([]string(nil), hook.Command...),
			TimeoutSeconds: hook.TimeoutSeconds,
		}
		configuration := backinghook.Configuration{}
		if input.Action == agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_START {
			if hook.Event != agentpb.BackingHookEvent_BACKING_HOOK_EVENT_AFTER_START {
				return invalidLifecycleWork()
			}
			configuration.AfterStart = definition
		} else {
			if hook.Event != agentpb.BackingHookEvent_BACKING_HOOK_EVENT_BEFORE_STOP {
				return invalidLifecycleWork()
			}
			configuration.BeforeStop = definition
		}
		if err := backinghook.ValidateConfiguration(configuration); err != nil {
			return err
		}
	}
	return nil
}

func invalidLifecycleWork() error {
	return errs.New(errs.KindValidationFailed, "Service lifecycle work has invalid or inconsistent captured authority")
}
