package executionplan

import (
	"crypto/sha256"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// CandidateRuntime is the self-contained prepared post-activation projection
// for one member of an immutable execution plan. It is not acknowledgement
// that the member executed successfully.
type CandidateRuntime struct {
	ServiceID             string `json:"service_id"`
	ReleaseID             string `json:"release_id"`
	Target                string `json:"target"`
	ProxyGeneration       uint64 `json:"proxy_generation"`
	ProxyConfigSHA256     []byte `json:"proxy_config_sha256,omitempty"`
	CurrentArtifact       []byte `json:"current_artifact"`
	RetainedPriorArtifact []byte `json:"retained_prior_artifact,omitempty"`
}

type candidateRuntimeActivation struct {
	target       string
	proxySwitch  *agentpb.ServiceProxySwitch
	proxyApplied bool
}

// PrepareCandidateRuntimes derives post-activation native runtime bytes only
// from a fully validated Deploy, Rollback or Blueprint plan. It never mutates
// the caller's plan or consults desired, historical, or observed state.
func PrepareCandidateRuntimes(plan *agentpb.ExecutionPlan) ([]CandidateRuntime, error) {
	sealed, err := Validate(plan)
	if err != nil {
		return nil, err
	}
	if !releaseOperation(sealed.GetOperation()) {
		return nil, errs.New(errs.KindValidationFailed, "candidate runtime requires a Release activation plan")
	}
	procedure := sealed.GetCandidateReleaseProcedure()
	if procedure == nil || len(procedure.GetMembers()) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "candidate runtime requires a candidate Release procedure")
	}
	artifacts := make(map[string]*agentpb.ComposeArtifact, len(sealed.GetArtifacts()))
	for _, artifact := range sealed.GetArtifacts() {
		artifacts[artifact.GetArtifactId()] = artifact
	}
	steps := make(map[string]*agentpb.ExecutionStep, len(sealed.GetSteps()))
	for _, step := range sealed.GetSteps() {
		steps[step.GetStepId()] = step
	}

	runtimes := make([]CandidateRuntime, len(procedure.GetMembers()))
	for index, member := range procedure.GetMembers() {
		if member.GetRolling() != nil {
			runtimes[index], err = prepareRollingCandidateRuntime(member, artifacts)
			if err != nil {
				return nil, err
			}
			continue
		}
		activation, activationErr := openCandidateRuntimeActivation(member, steps, artifacts)
		if activationErr != nil {
			return nil, activationErr
		}
		candidate := artifacts[member.GetCandidateArtifactId()]
		if activation.proxySwitch != nil && !activation.proxyApplied {
			prior := member.GetServingPredecessor()
			if prior == nil {
				return nil, invalidCandidateRuntimeProxySource()
			}
			workload := releaseTargetService(candidate, member.GetServiceId(), activation.target)
			if workload == nil {
				return nil, invalidCandidateRuntimeProxySource()
			}
			candidate, activationErr = retainCandidateRuntimeProxy(
				candidate,
				artifacts[prior.GetPriorArtifactId()],
				member.GetServiceId(),
				[]string{workload.GetComposeName()},
			)
			if activationErr != nil {
				return nil, activationErr
			}
		}
		current, proxyGeneration, proxyHash, projectionErr := projectCandidateRuntimeArtifact(
			candidate,
			member.GetServiceId(),
			member.GetCandidateReleaseId(),
			activation.target,
			true,
			activation.proxySwitch,
		)
		if projectionErr != nil {
			return nil, projectionErr
		}
		currentBytes, marshalErr := marshalCandidateRuntimeArtifact(current)
		if marshalErr != nil {
			return nil, marshalErr
		}
		retainedBytes, retainedErr := prepareCandidateRetainedPrior(
			member,
			activation.target,
			artifacts,
		)
		if retainedErr != nil {
			return nil, retainedErr
		}
		if err := ValidateNativePredecessorWitness(
			current.GetOwnerId(),
			member.GetServiceId(),
			currentBytes,
			retainedBytes,
		); err != nil {
			return nil, errs.New(
				errs.KindValidationFailed,
				"candidate runtime does not form a valid native predecessor witness",
			)
		}
		runtimes[index] = CandidateRuntime{
			ServiceID: member.GetServiceId(), ReleaseID: member.GetCandidateReleaseId(),
			Target: activation.target, ProxyGeneration: proxyGeneration,
			ProxyConfigSHA256: slices.Clone(proxyHash), CurrentArtifact: currentBytes,
			RetainedPriorArtifact: retainedBytes,
		}
	}
	return runtimes, nil
}

func openCandidateRuntimeActivation(
	member *agentpb.CandidateReleaseMember,
	steps map[string]*agentpb.ExecutionStep,
	artifacts map[string]*agentpb.ComposeArtifact,
) (candidateRuntimeActivation, error) {
	forward := make([]*agentpb.ExecutionStep, len(member.GetForwardStepIds()))
	for index, stepID := range member.GetForwardStepIds() {
		forward[index] = steps[stepID]
		if forward[index] == nil {
			return candidateRuntimeActivation{}, errs.New(
				errs.KindValidationFailed,
				"candidate runtime forward step is absent",
			)
		}
	}
	var workloadApply *agentpb.ComposeWorkloadApply
	var workloadWait *agentpb.WaitWorkloadHealthy
	var proxySwitch *agentpb.ServiceProxySwitch
	for _, step := range forward {
		switch {
		case step.GetComposeWorkloadApply() != nil:
			if workloadApply != nil {
				return candidateRuntimeActivation{}, invalidCandidateRuntimeForwardSteps()
			}
			workloadApply = step.GetComposeWorkloadApply()
		case step.GetWaitWorkloadHealthy() != nil:
			if workloadWait != nil {
				return candidateRuntimeActivation{}, invalidCandidateRuntimeForwardSteps()
			}
			workloadWait = step.GetWaitWorkloadHealthy()
		case step.GetServiceProxySwitch() != nil:
			if proxySwitch != nil {
				return candidateRuntimeActivation{}, invalidCandidateRuntimeForwardSteps()
			}
			proxySwitch = step.GetServiceProxySwitch()
		}
	}
	if workloadApply != nil || workloadWait != nil || proxySwitch != nil {
		if len(forward) != 3 || workloadApply == nil || workloadWait == nil || proxySwitch == nil ||
			workloadApply.GetArtifactId() != member.GetCandidateArtifactId() ||
			workloadApply.GetServiceId() != member.GetServiceId() ||
			workloadWait.GetArtifactId() != member.GetCandidateArtifactId() ||
			workloadWait.GetServiceId() != member.GetServiceId() ||
			workloadWait.GetTarget() != workloadApply.GetTarget() ||
			proxySwitch.GetCandidateArtifactId() != member.GetCandidateArtifactId() ||
			proxySwitch.GetServiceId() != member.GetServiceId() ||
			proxySwitch.GetReleaseId() != member.GetCandidateReleaseId() ||
			proxySwitch.GetToTarget() != workloadApply.GetTarget() {
			return candidateRuntimeActivation{}, invalidCandidateRuntimeForwardSteps()
		}
		proxy := releaseRuntimeService(
			artifacts[member.GetCandidateArtifactId()],
			member.GetServiceId(),
			"",
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
		)
		generation, err := ProxyConfigGeneration(
			proxy.GetProxyImplementation(),
			proxySwitch.GetConfigJson(),
			proxySwitch.GetReleaseId(),
		)
		if err != nil || generation != proxySwitch.GetProxyGeneration() {
			return candidateRuntimeActivation{}, errs.New(
				errs.KindValidationFailed,
				"candidate runtime proxy switch generation is inconsistent",
			)
		}
		return candidateRuntimeActivation{
			target: workloadApply.GetTarget(), proxySwitch: proxySwitch, proxyApplied: workloadApply.GetEnsureProxy(),
		}, nil
	}
	return openRecreateCandidateRuntimeActivation(member, forward)
}

func openRecreateCandidateRuntimeActivation(
	member *agentpb.CandidateReleaseMember,
	forward []*agentpb.ExecutionStep,
) (candidateRuntimeActivation, error) {
	var apply *agentpb.ComposeApply
	var wait *agentpb.WaitHealthy
	var acknowledge *agentpb.ServiceRecreateAcknowledge
	var remove *agentpb.ComposeRemove
	for _, step := range forward {
		switch {
		case step.GetComposeApply() != nil:
			if apply != nil {
				return candidateRuntimeActivation{}, invalidCandidateRuntimeForwardSteps()
			}
			apply = step.GetComposeApply()
		case step.GetWaitHealthy() != nil:
			if wait != nil {
				return candidateRuntimeActivation{}, invalidCandidateRuntimeForwardSteps()
			}
			wait = step.GetWaitHealthy()
		case step.GetServiceRecreateAcknowledge() != nil:
			if acknowledge != nil {
				return candidateRuntimeActivation{}, invalidCandidateRuntimeForwardSteps()
			}
			acknowledge = step.GetServiceRecreateAcknowledge()
		case step.GetComposeRemove() != nil:
			if remove != nil {
				return candidateRuntimeActivation{}, invalidCandidateRuntimeForwardSteps()
			}
			remove = step.GetComposeRemove()
		default:
			return candidateRuntimeActivation{}, invalidCandidateRuntimeForwardSteps()
		}
	}
	if len(forward) != 3 || apply == nil || acknowledge == nil ||
		apply.GetArtifactId() != member.GetCandidateArtifactId() ||
		!apply.GetForceRecreate() || !apply.GetNoDependencies() ||
		!candidateRuntimeSingleService(apply.GetServiceIds(), member.GetServiceId()) ||
		acknowledge.GetArtifactId() != member.GetCandidateArtifactId() ||
		acknowledge.GetServiceId() != member.GetServiceId() ||
		acknowledge.GetReleaseId() != member.GetCandidateReleaseId() {
		return candidateRuntimeActivation{}, invalidCandidateRuntimeForwardSteps()
	}
	prior := member.GetServingPredecessor()
	if prior.GetPriorArtifactId() == "" {
		if remove != nil || wait == nil || wait.GetArtifactId() != member.GetCandidateArtifactId() ||
			!candidateRuntimeSingleService(wait.GetServiceIds(), member.GetServiceId()) {
			return candidateRuntimeActivation{}, invalidCandidateRuntimeForwardSteps()
		}
	} else if wait != nil || remove == nil || remove.GetArtifactId() != prior.GetPriorArtifactId() ||
		!candidateRuntimeSingleService(remove.GetServiceIds(), member.GetServiceId()) {
		return candidateRuntimeActivation{}, invalidCandidateRuntimeForwardSteps()
	}
	return candidateRuntimeActivation{target: "singleton"}, nil
}

func projectCandidateRuntimeArtifact(
	source *agentpb.ComposeArtifact,
	serviceID, releaseID, target string,
	includeProxy bool,
	proxySwitch *agentpb.ServiceProxySwitch,
) (*agentpb.ComposeArtifact, uint64, []byte, error) {
	if source == nil || source.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT {
		return nil, 0, nil, errs.New(
			errs.KindValidationFailed,
			"candidate runtime requires an environment-owned Compose artifact",
		)
	}
	workload := releaseTargetService(source, serviceID, target)
	if workload == nil || expectedReleaseLabel(workload) != releaseID || workload.GetOwnerComponentId() != "" {
		return nil, 0, nil, errs.New(
			errs.KindValidationFailed,
			"candidate runtime workload does not match its sealed Release and target",
		)
	}
	if candidateRuntimeServiceCount(source, serviceID, target, workload.GetRole()) != 1 {
		return nil, 0, nil, errs.New(errs.KindValidationFailed, "candidate runtime workload is ambiguous")
	}
	var proxy *agentpb.ComposeService
	if includeProxy {
		proxy = releaseRuntimeService(
			source,
			serviceID,
			"",
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
		)
	}
	if proxySwitch != nil && proxy == nil {
		return nil, 0, nil, errs.New(errs.KindValidationFailed, "candidate runtime stable proxy is absent")
	}
	if proxy != nil && candidateRuntimeServiceCount(
		source,
		serviceID,
		"",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	) != 1 {
		return nil, 0, nil, errs.New(errs.KindValidationFailed, "candidate runtime stable proxy is ambiguous")
	}

	owned := proto.CloneOf(source)
	owned.Services = []*agentpb.ComposeService{proto.CloneOf(workload)}
	var proxyGeneration uint64
	var proxyHash []byte
	var sourceProxyConfig []byte
	if proxy != nil {
		sourceProxyConfig = slices.Clone(proxy.GetProxyConfigJson())
		ownedProxy := proto.CloneOf(proxy)
		if proxySwitch != nil {
			ownedProxy.ProxyConfigJson = slices.Clone(proxySwitch.GetConfigJson())
			ownedProxy.ProxyConfigSha256 = slices.Clone(proxySwitch.GetConfigSha256())
			proxyGeneration = proxySwitch.GetProxyGeneration()
		} else {
			generation, err := ProxyConfigGeneration(ownedProxy.GetProxyImplementation(), ownedProxy.GetProxyConfigJson(), releaseID)
			if err != nil {
				return nil, 0, nil, errs.New(
					errs.KindValidationFailed,
					"candidate runtime proxy configuration does not identify its Release",
				)
			}
			proxyGeneration = generation
		}
		proxyHash = slices.Clone(ownedProxy.GetProxyConfigSha256())
		owned.Services = append(owned.Services, ownedProxy)
		proxy = ownedProxy
	}
	if err := sealCandidateRuntimeProjection(source, owned, proxy, sourceProxyConfig); err != nil {
		return nil, 0, nil, err
	}
	return owned, proxyGeneration, proxyHash, nil
}

func sealCandidateRuntimeProjection(
	source, owned *agentpb.ComposeArtifact,
	proxy *agentpb.ComposeService,
	sourceProxyConfig []byte,
) error {
	slices.SortFunc(owned.Services, func(left, right *agentpb.ComposeService) int {
		return strings.Compare(left.GetComposeName(), right.GetComposeName())
	})
	canonical, references, err := projectCandidateRuntimeYAML(source, owned.Services, proxy, sourceProxyConfig)
	if err != nil {
		return err
	}
	owned.CanonicalYaml = canonical
	digest := sha256.Sum256(canonical)
	owned.YamlSha256 = slices.Clone(digest[:])
	owned.Networks = candidateRuntimeNetworks(source.GetNetworks(), references["networks"])
	owned.Volumes = candidateRuntimeVolumes(source.GetVolumes(), references["volumes"])
	return nil
}

func candidateRuntimeServiceCount(
	artifact *agentpb.ComposeArtifact,
	serviceID, target string,
	role agentpb.ComposeServiceRole,
) int {
	slot := target
	if target == "singleton" {
		slot = ""
	}
	count := 0
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == serviceID && service.GetRole() == role && service.GetSlot() == slot {
			count++
		}
	}
	return count
}

func prepareCandidateRetainedPrior(
	member *agentpb.CandidateReleaseMember,
	candidateTarget string,
	artifacts map[string]*agentpb.ComposeArtifact,
) ([]byte, error) {
	prior := member.GetServingPredecessor()
	if prior == nil || !candidateRuntimeOppositeSlots(candidateTarget, prior.GetPriorTarget()) {
		return nil, nil
	}
	artifact := artifacts[prior.GetPriorArtifactId()]
	retained, _, _, err := projectCandidateRuntimeArtifact(
		artifact,
		member.GetServiceId(),
		prior.GetPriorReleaseId(),
		prior.GetPriorTarget(),
		false,
		nil,
	)
	if err != nil {
		return nil, errs.New(
			errs.KindValidationFailed,
			"candidate runtime retained predecessor does not match sealed prior artifact",
		)
	}
	return marshalCandidateRuntimeArtifact(retained)
}

func candidateRuntimeNetworks(
	values []*agentpb.ComposeNetwork,
	referenced map[string]bool,
) []*agentpb.ComposeNetwork {
	result := make([]*agentpb.ComposeNetwork, 0, len(referenced))
	for _, value := range values {
		if referenced[value.GetComposeName()] {
			result = append(result, proto.CloneOf(value))
		}
	}
	return result
}

func candidateRuntimeVolumes(
	values []*agentpb.ComposeVolume,
	referenced map[string]bool,
) []*agentpb.ComposeVolume {
	result := make([]*agentpb.ComposeVolume, 0, len(referenced))
	for _, value := range values {
		if referenced[value.GetComposeName()] {
			result = append(result, proto.CloneOf(value))
		}
	}
	return result
}
