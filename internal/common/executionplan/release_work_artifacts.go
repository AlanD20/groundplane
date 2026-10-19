package executionplan

import (
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ReleaseWorkArtifacts is the selected runtime set for native Release execution.
// Unselected artifact members never become executable targets. Callers must use
// explicit Compose names without dependency expansion or orphan removal.
type ReleaseWorkArtifacts struct {
	Candidate *agentpb.ComposeArtifact
	Workloads []*agentpb.ComposeService
	Proxy     *agentpb.ComposeService
	Prior     *NativeServingRuntime
	Rolling   *rollingrelease.Plan
}

// OpenReleaseWorkArtifacts consumes typed Release input directly. It neither
// constructs an ExecutionPlan nor authorizes filesystem/container mutations.
func OpenReleaseWorkArtifacts(
	attempt workexecution.Attempt,
	input *agentpb.ReleaseWorkInput,
) (ReleaseWorkArtifacts, error) {
	var result ReleaseWorkArtifacts
	if input == nil || attempt.Validate() != nil || input.Candidate == nil || input.Predecessor == nil ||
		input.Candidate.OwnerId != input.EnvironmentId || input.RenderGeneration == 0 {
		return result, invalidReleaseWorkArtifacts()
	}
	if err := RejectUnknown(input); err != nil {
		return result, err
	}
	if err := validateArtifactContent(input.Candidate); err != nil {
		return result, err
	}
	result.Candidate = input.Candidate
	names := make(map[string]bool, len(input.Candidate.Services))
	for _, service := range input.Candidate.Services {
		if service == nil || validateComposeName(service.ComposeName) != nil || names[service.ComposeName] {
			return ReleaseWorkArtifacts{}, invalidReleaseWorkArtifacts()
		}
		names[service.ComposeName] = true
		if service.ServiceId != input.ServiceId {
			continue
		}
		if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			if result.Proxy != nil || validateNativePredecessorProxy(service) != nil {
				return ReleaseWorkArtifacts{}, invalidReleaseWorkArtifacts()
			}
			if _, err := OpenProxyConfiguration(service); err != nil {
				return ReleaseWorkArtifacts{}, err
			}
			result.Proxy = service
			continue
		}
		if nativePredecessorReleaseID(service) != input.ReleaseId {
			continue
		}
		labels, err := nativePredecessorLabels(service.ExpectedLabels)
		if err != nil || labels[labelEnvironmentID] != input.EnvironmentId || labels[labelPlanID] != attempt.Input.ID ||
			labels[labelRenderGen] != strconv.FormatUint(input.RenderGeneration, 10) {
			return ReleaseWorkArtifacts{}, invalidReleaseWorkArtifacts()
		}
		result.Workloads = append(result.Workloads, service)
	}
	if err := validateNativeWorkloadSet(result.Workloads); err != nil {
		return ReleaseWorkArtifacts{}, err
	}
	expectedRole := agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON
	switch input.Strategy {
	case agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE:
	case agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_BLUE_GREEN:
		expectedRole = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT
		if result.Proxy == nil {
			return ReleaseWorkArtifacts{}, invalidReleaseWorkArtifacts()
		}
	case agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_ROLLING:
		expectedRole = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA
	default:
		return ReleaseWorkArtifacts{}, invalidReleaseWorkArtifacts()
	}
	if result.Workloads[0].Role != expectedRole {
		return ReleaseWorkArtifacts{}, invalidReleaseWorkArtifacts()
	}
	prior := input.Predecessor
	if !prior.Absent {
		runtime, err := OpenNativeServingRuntime(
			input.EnvironmentId,
			input.ServiceId,
			prior.CurrentArtifact,
			prior.RetainedArtifact,
		)
		if err != nil {
			return ReleaseWorkArtifacts{}, err
		}
		if runtime.ReleaseID != prior.ReleaseId || runtime.Target != prior.Target ||
			runtime.ReleaseID == input.ReleaseId {
			return ReleaseWorkArtifacts{}, invalidReleaseWorkArtifacts()
		}
		if err := validateArtifactContent(runtime.Artifact); err != nil {
			return ReleaseWorkArtifacts{}, err
		}
		if runtime.Retained != nil {
			if err := validateArtifactContent(runtime.Retained); err != nil {
				return ReleaseWorkArtifacts{}, err
			}
		}
		result.Prior = &runtime
	}
	if input.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_ROLLING {
		plan, err := workexecution.DecodeReleaseRolling(input)
		if err != nil {
			return ReleaseWorkArtifacts{}, err
		}
		artifacts := []*agentpb.ComposeArtifact{input.Candidate}
		if result.Prior != nil {
			artifacts = append(artifacts, result.Prior.Artifact)
			if result.Prior.Retained != nil {
				artifacts = append(artifacts, result.Prior.Retained)
			}
		}
		if err := ValidateRollingArtifacts(plan, artifacts); err != nil {
			return ReleaseWorkArtifacts{}, err
		}
		if len(plan.Candidate) != len(result.Workloads) {
			return ReleaseWorkArtifacts{}, invalidReleaseWorkArtifacts()
		}
		result.Rolling = &plan
	}
	if err := validateReleaseWorkTraffic(input, result); err != nil {
		return ReleaseWorkArtifacts{}, err
	}
	return result, nil
}

func invalidReleaseWorkArtifacts() error {
	return errs.New(
		errs.KindValidationFailed,
		"Release work artifacts do not match the selected Service, Release or captured runtime",
	)
}
