package executionplan

import (
	"crypto/sha256"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
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
// from a fully validated ordinary Deploy or Rollback plan. It never mutates
// the caller's plan or consults desired, historical, or observed state.
func PrepareCandidateRuntimes(plan *agentpb.ExecutionPlan) ([]CandidateRuntime, error) {
	sealed, err := Validate(plan)
	if err != nil {
		return nil, err
	}
	if !releaseOperation(sealed.GetOperation()) {
		return nil, errs.New(errs.KindValidationFailed, "candidate runtime requires an ordinary Release plan")
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
		activation, activationErr := openCandidateRuntimeActivation(member, steps)
		if activationErr != nil {
			return nil, activationErr
		}
		candidate := artifacts[member.GetCandidateArtifactId()]
		if activation.proxySwitch != nil && !activation.proxyApplied {
			prior := member.GetServingPredecessor()
			if prior == nil {
				return nil, invalidCandidateRuntimeProxySource()
			}
			candidate, activationErr = retainCandidateRuntimeProxy(
				candidate,
				artifacts[prior.GetPriorArtifactId()],
				member.GetServiceId(),
				activation.target,
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
		generation, err := ProxyConfigGeneration(proxySwitch.GetConfigJson(), proxySwitch.GetReleaseId())
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
	if prior == nil {
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
			generation, err := ProxyConfigGeneration(ownedProxy.GetProxyConfigJson(), releaseID)
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
	slices.SortFunc(owned.Services, func(left, right *agentpb.ComposeService) int {
		return strings.Compare(left.GetComposeName(), right.GetComposeName())
	})
	canonical, references, err := projectCandidateRuntimeYAML(source, owned.Services, proxy, sourceProxyConfig)
	if err != nil {
		return nil, 0, nil, err
	}
	owned.CanonicalYaml = canonical
	digest := sha256.Sum256(canonical)
	owned.YamlSha256 = slices.Clone(digest[:])
	owned.Networks = candidateRuntimeNetworks(source.GetNetworks(), references["networks"])
	owned.Volumes = candidateRuntimeVolumes(source.GetVolumes(), references["volumes"])
	return owned, proxyGeneration, proxyHash, nil
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

func projectCandidateRuntimeYAML(
	source *agentpb.ComposeArtifact,
	selected []*agentpb.ComposeService,
	proxy *agentpb.ComposeService,
	sourceProxyConfig []byte,
) ([]byte, map[string]map[string]bool, error) {
	var document yaml.Node
	if yaml.Unmarshal(source.GetCanonicalYaml(), &document) != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode {
		return nil, nil, errs.New(errs.KindValidationFailed, "candidate runtime Compose YAML is invalid")
	}
	root := document.Content[0]
	services := candidateRuntimeMappingValue(root, "services")
	if services == nil || services.Kind != yaml.MappingNode {
		return nil, nil, errs.New(errs.KindValidationFailed, "candidate runtime Compose Service mapping is absent")
	}
	wanted := make(map[string]bool, len(selected))
	for _, service := range selected {
		if service == nil || service.GetComposeName() == "" || wanted[service.GetComposeName()] {
			return nil, nil, errs.New(errs.KindValidationFailed, "candidate runtime Service selection is invalid")
		}
		wanted[service.GetComposeName()] = true
	}
	kept := make([]*yaml.Node, 0, len(selected)*2)
	found := make(map[string]bool, len(selected))
	references := make(map[string]map[string]bool, 4)
	var proxyNode *yaml.Node
	for index := 0; index+1 < len(services.Content); index += 2 {
		name, value := services.Content[index], services.Content[index+1]
		if name.Kind != yaml.ScalarNode || !wanted[name.Value] {
			continue
		}
		if found[name.Value] || value.Kind != yaml.MappingNode {
			return nil, nil, errs.New(errs.KindValidationFailed, "candidate runtime Compose Service is ambiguous")
		}
		found[name.Value] = true
		kept = append(kept, name, value)
		if err := candidateRuntimeResourceReferences(value, references); err != nil {
			return nil, nil, err
		}
		if proxy != nil && name.Value == proxy.GetComposeName() {
			proxyNode = value
		}
	}
	if len(found) != len(wanted) {
		return nil, nil, errs.New(errs.KindValidationFailed, "candidate runtime Compose Service is absent")
	}
	if proxy != nil {
		if proxyNode == nil || candidateRuntimeSetProxyConfig(root, proxyNode, proxy, sourceProxyConfig) != nil {
			return nil, nil, errs.New(
				errs.KindValidationFailed,
				"candidate runtime proxy YAML does not bind its sealed configuration",
			)
		}
	}
	services.Content = kept
	for _, section := range []string{"networks", "volumes", "configs", "secrets"} {
		candidateRuntimePruneResources(root, section, references[section])
	}
	encoded, err := yaml.Marshal(&document)
	if err != nil {
		return nil, nil, errs.Wrap(errs.KindInternal, err)
	}
	return encoded, references, nil
}

func candidateRuntimeResourceReferences(service *yaml.Node, references map[string]map[string]bool) error {
	for _, section := range []string{"networks", "volumes", "configs", "secrets"} {
		entries := candidateRuntimeMappingValue(service, section)
		if entries == nil {
			continue
		}
		if references[section] == nil {
			references[section] = make(map[string]bool)
		}
		if section == "networks" && entries.Kind == yaml.MappingNode {
			for index := 0; index+1 < len(entries.Content); index += 2 {
				if entries.Content[index].Kind != yaml.ScalarNode {
					return errs.New(errs.KindValidationFailed, "candidate runtime network reference is invalid")
				}
				references[section][entries.Content[index].Value] = true
			}
			continue
		}
		if entries.Kind != yaml.SequenceNode {
			return errs.New(errs.KindValidationFailed, "candidate runtime resource references are invalid")
		}
		for _, entry := range entries.Content {
			name, include, err := candidateRuntimeResourceReference(section, entry)
			if err != nil {
				return err
			}
			if include {
				references[section][name] = true
			}
		}
	}
	return nil
}

func candidateRuntimeResourceReference(section string, entry *yaml.Node) (string, bool, error) {
	if entry.Kind == yaml.ScalarNode {
		name := entry.Value
		if section == "volumes" {
			name, _, _ = strings.Cut(name, ":")
		}
		return name, name != "", nil
	}
	if entry.Kind != yaml.MappingNode {
		return "", false, errs.New(errs.KindValidationFailed, "candidate runtime resource reference is invalid")
	}
	if section == "volumes" {
		kind := candidateRuntimeMappingValue(entry, "type")
		if kind != nil && kind.Value != "volume" {
			return "", false, nil
		}
	}
	source := candidateRuntimeMappingValue(entry, "source")
	if source == nil || source.Kind != yaml.ScalarNode || source.Value == "" {
		return "", false, errs.New(errs.KindValidationFailed, "candidate runtime resource source is invalid")
	}
	return source.Value, true, nil
}

func candidateRuntimeSetProxyConfig(
	root, service *yaml.Node,
	proxy *agentpb.ComposeService,
	sourceProxyConfig []byte,
) error {
	configs := candidateRuntimeMappingValue(service, "configs")
	definitions := candidateRuntimeMappingValue(root, "configs")
	if configs == nil || configs.Kind != yaml.SequenceNode || definitions == nil ||
		definitions.Kind != yaml.MappingNode {
		return invalidCandidateRuntimeProxyConfig()
	}
	matches := 0
	var content *yaml.Node
	for _, entry := range configs.Content {
		name, include, err := candidateRuntimeResourceReference("configs", entry)
		if err != nil || !include {
			return invalidCandidateRuntimeProxyConfig()
		}
		definition := candidateRuntimeMappingValue(definitions, name)
		candidate := candidateRuntimeMappingValue(definition, "content")
		if candidate == nil || candidate.Kind != yaml.ScalarNode || candidate.Value != string(sourceProxyConfig) {
			continue
		}
		matches++
		content = candidate
	}
	if matches != 1 || content == nil {
		return invalidCandidateRuntimeProxyConfig()
	}
	content.Tag = "!!str"
	content.Value = string(proxy.GetProxyConfigJson())
	return nil
}

func invalidCandidateRuntimeProxyConfig() error {
	return errs.New(errs.KindValidationFailed, "candidate runtime proxy configuration is invalid")
}

func candidateRuntimePruneResources(root *yaml.Node, section string, referenced map[string]bool) {
	index := candidateRuntimeMappingIndex(root, section)
	if index < 0 {
		return
	}
	resources := root.Content[index+1]
	if resources.Kind != yaml.MappingNode {
		return
	}
	kept := resources.Content[:0]
	for offset := 0; offset+1 < len(resources.Content); offset += 2 {
		if referenced[resources.Content[offset].Value] {
			kept = append(kept, resources.Content[offset], resources.Content[offset+1])
		}
	}
	resources.Content = kept
	if len(kept) == 0 {
		root.Content = append(root.Content[:index], root.Content[index+2:]...)
	}
}

func candidateRuntimeMappingValue(mapping *yaml.Node, key string) *yaml.Node {
	index := candidateRuntimeMappingIndex(mapping, key)
	if index < 0 {
		return nil
	}
	return mapping.Content[index+1]
}

func candidateRuntimeMappingIndex(mapping *yaml.Node, key string) int {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return -1
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Kind == yaml.ScalarNode && mapping.Content[index].Value == key {
			return index
		}
	}
	return -1
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
