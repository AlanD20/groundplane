package workexecution

import (
	"bytes"
	"crypto/sha256"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
)

// DecodeReleaseDelivery checks the delivery envelope and owns its bytes. This
// does not validate Compose execution authority or grant effects: the Release
// executor must verify its artifacts and obtain a durable start/checkpoint grant.
func DecodeReleaseDelivery(
	value *agentpb.ReleaseWorkAssignment,
	authenticated Identity,
) (Attempt, *agentpb.ReleaseWorkInput, error) {
	if value == nil || len(value.ProtoReflect().GetUnknown()) != 0 || value.Release == nil {
		return Attempt{}, nil, invalidReleaseDelivery()
	}
	attempt, err := DecodeAgentAttempt(value.Attempt, authenticated)
	if err != nil {
		return Attempt{}, nil, err
	}
	input := value.Release
	if len(input.ProtoReflect().GetUnknown()) != 0 || input.RenderGeneration == 0 ||
		ids.Validate(ids.KindOperation, input.OperationId) != nil ||
		ids.Validate(ids.KindEnvironment, input.EnvironmentId) != nil ||
		ids.Validate(ids.KindService, input.ServiceId) != nil ||
		ids.Validate(ids.KindDeployment, input.ReleaseId) != nil ||
		!validReleaseConfiguration(input.Configuration, input.EnvironmentId) ||
		input.PriorConfiguration != nil && !validReleaseConfiguration(input.PriorConfiguration, input.EnvironmentId) {
		return Attempt{}, nil, invalidReleaseDelivery()
	}
	if input.PriorConfiguration != nil && input.PriorConfiguration.Generation > input.Configuration.Generation {
		return Attempt{}, nil, invalidReleaseDelivery()
	}
	if input.FailurePolicy != agentpb.WorkReleaseFailurePolicy_WORK_RELEASE_FAILURE_POLICY_SWITCH_BACK &&
		input.FailurePolicy != agentpb.WorkReleaseFailurePolicy_WORK_RELEASE_FAILURE_POLICY_LEAVE_ACTIVE {
		return Attempt{}, nil, invalidReleaseDelivery()
	}
	if input.PriorConfiguration != nil &&
		input.PriorConfiguration.ConfigurationId == input.Configuration.ConfigurationId &&
		!proto.Equal(input.PriorConfiguration, input.Configuration) {
		return Attempt{}, nil, invalidReleaseDelivery()
	}
	for index, destination := range input.ConfigurationDestinations {
		if destination == "" || len(destination) > 4096 || strings.ContainsRune(destination, 0) ||
			index > 0 && input.ConfigurationDestinations[index-1] >= destination {
			return Attempt{}, nil, invalidReleaseDelivery()
		}
	}
	artifact := input.Candidate
	if artifact == nil || len(artifact.ProtoReflect().GetUnknown()) != 0 ||
		ids.Validate(ids.KindConfig, artifact.ArtifactId) != nil ||
		artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.OwnerId != input.EnvironmentId ||
		artifact.ProjectName != "gp-"+strings.ToLower(input.EnvironmentId) ||
		len(artifact.CanonicalYaml) == 0 {
		return Attempt{}, nil, invalidReleaseDelivery()
	}
	digest := sha256.Sum256(artifact.CanonicalYaml)
	if !bytes.Equal(digest[:], artifact.YamlSha256) {
		return Attempt{}, nil, invalidReleaseDelivery()
	}
	prior := input.Predecessor
	if prior == nil || len(prior.ProtoReflect().GetUnknown()) != 0 {
		return Attempt{}, nil, invalidReleaseDelivery()
	}
	if prior.Absent {
		if prior.ReleaseId != "" || prior.Target != "" || len(prior.CurrentArtifact) != 0 ||
			len(prior.RetainedArtifact) != 0 {
			return Attempt{}, nil, invalidReleaseDelivery()
		}
	} else if ids.Validate(ids.KindDeployment, prior.ReleaseId) != nil || prior.ReleaseId == input.ReleaseId ||
		len(prior.CurrentArtifact) == 0 || prior.Target != "singleton" && prior.Target != "blue" && prior.Target != "green" && prior.Target != "rolling" {
		return Attempt{}, nil, invalidReleaseDelivery()
	}
	switch input.Strategy {
	case agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE,
		agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_BLUE_GREEN:
		if input.Rolling != nil {
			return Attempt{}, nil, invalidReleaseDelivery()
		}
	case agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_ROLLING:
		if _, err := DecodeReleaseRolling(input); err != nil {
			return Attempt{}, nil, err
		}
	default:
		return Attempt{}, nil, invalidReleaseDelivery()
	}
	if len(input.Hooks) > 16 {
		return Attempt{}, nil, invalidReleaseDelivery()
	}
	executions := make(map[string]bool, len(input.Hooks))
	deployHooks, rollbackHooks := false, false
	for _, hook := range input.Hooks {
		if hook == nil || len(hook.ProtoReflect().GetUnknown()) != 0 || hook.Input == nil ||
			len(hook.Input.ProtoReflect().GetUnknown()) != 0 || len(hook.Input.Sha256) != sha256.Size ||
			ids.Validate(
				ids.KindPlan,
				hook.Input.InputId,
			) != nil || ids.Validate(ids.KindScript, hook.ScriptId) != nil || hook.Order > 65535 {
			return Attempt{}, nil, invalidReleaseDelivery()
		}
		if _, err := ulid.ParseStrict(hook.ExecutionId); err != nil || executions[hook.ExecutionId] {
			return Attempt{}, nil, invalidReleaseDelivery()
		}
		executions[hook.ExecutionId] = true
		switch hook.Phase {
		case "pre-deploy", "post-deploy":
			deployHooks = true
		case "pre-rollback", "post-rollback":
			rollbackHooks = true
		case "on-failure":
		default:
			return Attempt{}, nil, invalidReleaseDelivery()
		}
	}
	if deployHooks && rollbackHooks {
		return Attempt{}, nil, invalidReleaseDelivery()
	}
	return attempt, proto.Clone(input).(*agentpb.ReleaseWorkInput), nil
}

func validReleaseConfiguration(reference *agentpb.WorkConfigurationReference, environmentID string) bool {
	return reference != nil && len(reference.ProtoReflect().GetUnknown()) == 0 &&
		ids.Validate(ids.KindConfig, reference.ConfigurationId) == nil && reference.EnvironmentId == environmentID &&
		reference.Generation != 0 && len(reference.Sha256) == sha256.Size
}

func DecodeReleaseRolling(input *agentpb.ReleaseWorkInput) (rollingrelease.Plan, error) {
	if input == nil || input.Rolling == nil || len(input.Rolling.ProtoReflect().GetUnknown()) != 0 ||
		input.Strategy != agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_ROLLING {
		return rollingrelease.Plan{}, invalidReleaseDelivery()
	}
	plan := rollingrelease.Plan{
		ServiceID:             input.ServiceId,
		ReleaseID:             input.ReleaseId,
		ReplicaTimeoutSeconds: input.Rolling.ReplicaTimeoutSeconds,
	}
	for index, group := range [][]*agentpb.RollingReplica{input.Rolling.Prior, input.Rolling.Candidate, input.Rolling.Retained} {
		var replicas []rollingrelease.Replica
		for _, replica := range group {
			if replica == nil || len(replica.ProtoReflect().GetUnknown()) != 0 {
				return rollingrelease.Plan{}, invalidReleaseDelivery()
			}
			replicas = append(
				replicas,
				rollingrelease.Replica{ReleaseID: replica.ReleaseId, ArtifactID: replica.ArtifactId,
					ComposeName: replica.ComposeName, Ordinal: replica.Ordinal, ContainerID: replica.ContainerId,
					ContainerName: replica.ContainerName, ComposeReplica: replica.ComposeReplica},
			)
		}
		switch index {
		case 0:
			plan.Prior = replicas
		case 1:
			plan.Candidate = replicas
		case 2:
			plan.Retained = replicas
		}
	}
	if err := plan.Validate(); err != nil {
		return rollingrelease.Plan{}, err
	}
	return plan, nil
}

func invalidReleaseDelivery() error {
	return errs.New(
		errs.KindValidationFailed,
		"Release work delivery has invalid identity, configuration or captured runtime metadata",
	)
}
