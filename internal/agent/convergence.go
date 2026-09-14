package agent

import (
	"fmt"
	"sort"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type composeConvergence struct {
	Ready   bool
	Summary string
}

// Blueprint reapply can reconcile Components without a native candidate Release
// procedure. Its selected Component health has the same shared-project boundary.
func blueprintManagedHealthSelection(
	plan *agentpb.ExecutionPlan,
	artifact *agentpb.ComposeArtifact,
	selected []string,
) bool {
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY || len(selected) == 0 {
		return false
	}
	services, err := convergenceServices(artifact, selected)
	if err != nil {
		return false
	}
	for _, service := range services {
		if service.GetOwnerComponentId() == "" ||
			service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED {
			return false
		}
	}
	return true
}

// evaluateComposeConvergence is the pure decision behind WaitHealthy. The
// observer supplies evidence; this function never queries Docker and never
// guesses whether a partial or healthcheck-less project is ready.
func evaluateComposeConvergence(
	artifact *agentpb.ComposeArtifact,
	observed *agentpb.ObservedProject,
	selectedServices []string,
) (composeConvergence, error) {
	return evaluateSelectedComposeConvergence(artifact, observed, selectedServices, true)
}

// Sealed lifecycle procedures reconcile selected services within a shared
// Compose project. Unrelated containers can legitimately retain prior plan
// labels. Only a named container outside the selected names and stable Service
// ids is irrelevant here. Named networks and volumes outside the artifact are
// also outside lifecycle authority; selected and unidentifiable collisions fail closed.
func evaluateLifecycleComposeConvergence(
	artifact *agentpb.ComposeArtifact,
	observed *agentpb.ObservedProject,
	selectedServices []string,
	requireHealthcheck bool,
) (composeConvergence, error) {
	if artifact == nil || observed == nil || len(selectedServices) == 0 {
		return evaluateSelectedComposeConvergence(artifact, observed, selectedServices, requireHealthcheck)
	}
	services, err := convergenceServices(artifact, selectedServices)
	if err != nil {
		return composeConvergence{}, err
	}
	scoped := scopeLifecycleComposeObservation(artifact, observed, services)
	return evaluateSelectedComposeConvergence(artifact, scoped, selectedServices, requireHealthcheck)
}

// scopeLifecycleComposeObservation keeps collision evidence that could affect
// the selected sealed services while excluding unrelated named resources in
// their shared Compose project. Unnamed, selected, and unknown-kind evidence
// remains fail-closed because it cannot be safely attributed elsewhere.
func scopeLifecycleComposeObservation(
	artifact *agentpb.ComposeArtifact,
	observed *agentpb.ObservedProject,
	selectedServices []*agentpb.ComposeService,
) *agentpb.ObservedProject {
	if artifact == nil || observed == nil {
		return observed
	}
	names := make(map[string]bool, len(selectedServices))
	serviceIDs := make(map[string]bool, len(selectedServices))
	for _, service := range selectedServices {
		if service == nil {
			continue
		}
		names[service.GetComposeName()] = true
		if service.GetServiceId() != "" {
			serviceIDs[service.GetServiceId()] = true
		}
	}
	networkNames := make(map[string]bool, len(artifact.GetNetworks())*2)
	for _, network := range artifact.GetNetworks() {
		networkNames[network.GetDockerName()] = true
		networkNames[network.GetComposeName()] = true
	}
	volumeNames := make(map[string]bool, len(artifact.GetVolumes())*2)
	for _, volume := range artifact.GetVolumes() {
		volumeNames[volume.GetDockerName()] = true
		volumeNames[volume.GetComposeName()] = true
	}
	scoped := proto.CloneOf(observed)
	scoped.Collisions = nil
	for _, collision := range observed.GetCollisions() {
		if collision.GetKind() == agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_VOLUME &&
			collision.GetName() != "" && !volumeNames[collision.GetName()] {
			continue
		}
		if collision.GetKind() == agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_NETWORK &&
			collision.GetName() != "" && !networkNames[collision.GetName()] {
			continue
		}
		if collision.GetKind() != agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER ||
			collision.GetComposeServiceName() == "" || names[collision.GetComposeServiceName()] ||
			serviceIDs[collision.GetServiceId()] {
			scoped.Collisions = append(scoped.Collisions, collision)
		}
	}
	return scoped
}

func evaluateSelectedComposeConvergence(
	artifact *agentpb.ComposeArtifact,
	observed *agentpb.ObservedProject,
	selectedServices []string,
	requireHealthcheck bool,
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
		if requireHealthcheck && !service.GetHasHealthcheck() {
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
			if service.GetHasHealthcheck() &&
				container.GetHealth() != agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY {
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

// evaluateReleaseWorkloadConvergence scopes collision evidence to the selected
// release workload. A partial release intentionally leaves unrelated project
// resources on their prior plan labels; selected containers still enter the
// observation only after their exact expected labels match the release plan.
func evaluateReleaseWorkloadConvergence(
	artifact *agentpb.ComposeArtifact,
	observed *agentpb.ObservedProject,
	selectedServices []string,
) (composeConvergence, error) {
	if observed == nil {
		return composeConvergence{}, errs.New(errs.KindInternal, "compose convergence input is incomplete")
	}
	scoped := proto.CloneOf(observed)
	scoped.Collisions = nil
	return evaluateSelectedComposeConvergence(artifact, scoped, selectedServices, false)
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
