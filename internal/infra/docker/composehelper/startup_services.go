package composehelper

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// StartupServices validates a request and selects only services its commands may
// start, including sealed Compose dependencies. It performs no external IO.
func StartupServices(request *agentpb.ComposeHelperRequest) ([]*agentpb.ComposeService, error) {
	owned, step, artifact, err := validateRequest(request)
	if err != nil {
		return nil, err
	}
	var names []string
	dependencies := false
	switch {
	case step.GetComposeApply() != nil:
		apply := step.GetComposeApply()
		dependencies = !apply.NoDependencies
		if apply.FullReconcile {
			for _, service := range artifact.Services {
				if service.ExpectedReplicas != 0 {
					names = append(names, service.ComposeName)
				}
			}
		} else {
			names, err = applyServiceNames(owned.Plan, step, artifact)
			if err != nil {
				return nil, err
			}
		}
	case step.GetComposeWorkloadApply() != nil:
		apply := step.GetComposeWorkloadApply()
		dependencies = true
		names = append(names, releaseWorkloadName(artifact, apply.ServiceId, apply.Target))
		names = append(names, releaseProxyName(artifact, apply.ServiceId))
	case step.GetServiceRecreateCompensate() != nil:
		names = serviceNames(artifact, []string{step.GetServiceRecreateCompensate().ServiceId})
	case step.GetServiceProxyCompensate() != nil:
		compensate := step.GetServiceProxyCompensate()
		if !compensate.Enabled || compensate.PriorArtifactId == "" {
			return nil, nil
		}
		artifact = composeArtifactByID(owned.Plan, compensate.PriorArtifactId)
		if artifact == nil {
			return nil, errs.New(errs.KindValidationFailed, "startup prior artifact is absent")
		}
		names = serviceNames(artifact, []string{compensate.ServiceId})
	case step.GetCandidateRestorationCompensate() != nil && executionplan.RestorationTargetForService(owned.GetRestorationAuthority(), restorationStepServiceID(step)) == agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR:
		var service *agentpb.ComposeService
		artifact, service, _, _, err = openServingPredecessor(owned, artifact, step)
		if err != nil {
			return nil, err
		}
		proxy, proxyErr := servingPredecessorProxy(artifact, service.GetServiceId())
		if proxyErr != nil {
			return nil, proxyErr
		}
		names = servingPredecessorNames(service, proxy)
	default:
		return nil, nil
	}
	return startupClosure(artifact, names, dependencies)
}

type startupDocument struct {
	Services map[string]startupService `yaml:"services"`
}

type startupService struct {
	DependsOn map[string]startupDependency `yaml:"depends_on"`
}

type startupDependency struct {
	Condition string `yaml:"condition"`
	Restart   bool   `yaml:"restart"`
	Required  bool   `yaml:"required"`
}

func startupClosure(
	artifact *agentpb.ComposeArtifact,
	names []string,
	dependencies bool,
) ([]*agentpb.ComposeService, error) {
	if len(names) == 0 {
		return nil, nil
	}
	byName := make(map[string]*agentpb.ComposeService, len(artifact.Services))
	for _, service := range artifact.Services {
		if service == nil || service.ComposeName == "" || byName[service.ComposeName] != nil {
			return nil, errs.New(errs.KindValidationFailed, "startup service mapping is invalid")
		}
		byName[service.ComposeName] = service
	}
	var document startupDocument
	if dependencies {
		if err := yaml.Unmarshal(artifact.CanonicalYaml, &document); err != nil {
			return nil, errs.Wrap(errs.KindValidationFailed, err)
		}
		if len(document.Services) != len(byName) {
			return nil, errs.New(errs.KindValidationFailed, "startup Compose service mapping differs from artifact")
		}
		for name, service := range document.Services {
			if byName[name] == nil {
				return nil, errs.New(errs.KindValidationFailed, "startup Compose service is absent from artifact")
			}
			for dependency, value := range service.DependsOn {
				if byName[dependency] == nil ||
					value.Condition != "service_started" && value.Condition != "service_healthy" &&
						value.Condition != "service_completed_successfully" {
					return nil, errs.New(errs.KindValidationFailed, "startup Compose dependency is invalid")
				}
			}
		}
	}
	selected := make(map[string]bool, len(byName))
	for len(names) != 0 {
		name := names[0]
		names = names[1:]
		if byName[name] == nil {
			return nil, errs.New(errs.KindValidationFailed, "startup selected service is absent")
		}
		if selected[name] {
			continue
		}
		selected[name] = true
		if dependencies {
			for dependency := range document.Services[name].DependsOn {
				if !selected[dependency] {
					names = append(names, dependency)
				}
			}
		}
	}
	var result []*agentpb.ComposeService
	for _, service := range artifact.Services {
		if selected[service.ComposeName] && service.ExpectedReplicas != 0 {
			result = append(result, proto.CloneOf(service))
		}
	}
	return result, nil
}
