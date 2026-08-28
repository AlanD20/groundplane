package agent

import (
	"fmt"
	"sort"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type composeConvergence struct {
	Ready   bool
	Summary string
}

// evaluateComposeConvergence is the pure decision behind WaitHealthy. The
// observer supplies evidence; this function never queries Docker and never
// guesses whether a partial or healthcheck-less project is ready.
func evaluateComposeConvergence(
	artifact *agentpb.ComposeArtifact,
	observed *agentpb.ObservedProject,
	selectedServices []string,
) (composeConvergence, error) {
	if artifact == nil || observed == nil {
		return composeConvergence{}, errs.New(errs.KindInternal, "compose convergence input is incomplete")
	}
	if artifact.GetProjectName() == "" || observed.GetProjectName() != artifact.GetProjectName() {
		return composeConvergence{}, errs.New(errs.KindInternal, "compose convergence project identity does not match")
	}
	if len(observed.GetCollisions()) > 0 {
		return composeConvergence{}, errs.New(
			errs.KindStateConflict,
			"compose project ownership collision prevents convergence",
		)
	}

	services, err := convergenceServices(artifact, selectedServices)
	if err != nil {
		return composeConvergence{}, err
	}
	containers := append([]*agentpb.ObservedContainer(nil), observed.GetContainers()...)
	sort.Slice(containers, func(left, right int) bool {
		if containers[left].GetServiceId() == containers[right].GetServiceId() {
			return containers[left].GetName() < containers[right].GetName()
		}
		return containers[left].GetServiceId() < containers[right].GetServiceId()
	})

	for _, service := range services {
		if !service.GetHasHealthcheck() {
			return composeConvergence{}, errs.Newf(
				errs.KindValidationFailed,
				"compose service %s cannot be used by WaitHealthy without a healthcheck",
				service.GetComposeName(),
			)
		}
		matching := containersForService(containers, service)
		if len(matching) != int(service.GetExpectedReplicas()) {
			return composeConvergence{
				Summary: fmt.Sprintf(
					"service %s has %d of %d expected replicas",
					service.GetComposeName(),
					len(matching),
					service.GetExpectedReplicas(),
				),
			}, nil
		}
		for _, container := range matching {
			if container.GetState() != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING {
				return composeConvergence{
					Summary: fmt.Sprintf(
						"service %s container %s is not running",
						service.GetComposeName(),
						container.GetName(),
					),
				}, nil
			}
			if container.GetHealth() != agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY {
				return composeConvergence{
					Summary: fmt.Sprintf(
						"service %s container %s is not healthy",
						service.GetComposeName(),
						container.GetName(),
					),
				}, nil
			}
		}
	}

	return composeConvergence{Ready: true, Summary: "all selected services are healthy"}, nil
}

func convergenceServices(
	artifact *agentpb.ComposeArtifact,
	selected []string,
) ([]*agentpb.ComposeService, error) {
	byName := make(map[string]*agentpb.ComposeService, len(artifact.GetServices()))
	for _, service := range artifact.GetServices() {
		if service != nil {
			byName[service.GetComposeName()] = service
		}
	}

	names := append([]string(nil), selected...)
	if len(names) == 0 {
		names = make([]string, 0, len(byName))
		for name := range byName {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "WaitHealthy requires at least one service")
	}
	sort.Strings(names)

	services := make([]*agentpb.ComposeService, 0, len(names))
	for index, name := range names {
		if index > 0 && names[index-1] == name {
			return nil, errs.New(errs.KindValidationFailed, "WaitHealthy service selection contains a duplicate")
		}
		service := byName[name]
		if service == nil {
			return nil, errs.Newf(errs.KindValidationFailed, "WaitHealthy service %s is not in the artifact", name)
		}
		services = append(services, service)
	}
	return services, nil
}

func containersForService(
	containers []*agentpb.ObservedContainer,
	service *agentpb.ComposeService,
) []*agentpb.ObservedContainer {
	matching := make([]*agentpb.ObservedContainer, 0, len(containers))
	for _, container := range containers {
		if container != nil && container.GetServiceId() == service.GetServiceId() &&
			labelPairsEqual(container.GetLabels(), service.GetExpectedLabels()) {
			matching = append(matching, container)
		}
	}
	return matching
}

func labelPairsEqual(left, right []*agentpb.LabelPair) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] == nil || right[index] == nil ||
			left[index].GetKey() != right[index].GetKey() ||
			left[index].GetValue() != right[index].GetValue() {
			return false
		}
	}
	return true
}
