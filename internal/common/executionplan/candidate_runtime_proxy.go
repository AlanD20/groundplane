package executionplan

import (
	"crypto/sha256"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

func retainCandidateRuntimeProxy(
	candidate *agentpb.ComposeArtifact,
	acknowledged *agentpb.ComposeArtifact,
	serviceID, target string,
) (*agentpb.ComposeArtifact, error) {
	if candidate == nil || acknowledged == nil ||
		candidate.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		acknowledged.GetOwnerKind() != candidate.GetOwnerKind() ||
		acknowledged.GetOwnerId() != candidate.GetOwnerId() ||
		acknowledged.GetProjectName() != candidate.GetProjectName() ||
		acknowledged.GetAuthorizedVolumeDir() != candidate.GetAuthorizedVolumeDir() {
		return nil, invalidCandidateRuntimeProxySource()
	}
	var sourceProxy *agentpb.ComposeService
	for _, service := range acknowledged.GetServices() {
		if service.GetServiceId() != serviceID ||
			service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		if sourceProxy != nil || validateNativePredecessorProxy(service) != nil {
			return nil, invalidCandidateRuntimeProxySource()
		}
		sourceProxy = service
	}
	workload := releaseTargetService(candidate, serviceID, target)
	if sourceProxy == nil || workload == nil {
		return nil, invalidCandidateRuntimeProxySource()
	}
	hybrid := proto.CloneOf(candidate)
	replaced := false
	for index, service := range hybrid.GetServices() {
		if service.GetServiceId() != serviceID ||
			service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		if replaced || service.GetComposeName() != sourceProxy.GetComposeName() {
			return nil, invalidCandidateRuntimeProxySource()
		}
		hybrid.Services[index] = proto.CloneOf(sourceProxy)
		replaced = true
	}
	if !replaced {
		return nil, invalidCandidateRuntimeProxySource()
	}
	canonical, references, err := retainCandidateRuntimeProxyYAML(
		candidate,
		workload.GetComposeName(),
		acknowledged,
		sourceProxy.GetComposeName(),
	)
	if err != nil {
		return nil, err
	}
	hybrid.CanonicalYaml = canonical
	digest := sha256.Sum256(canonical)
	hybrid.YamlSha256 = digest[:]
	hybrid.Networks, err = attachRuntimeNetworks(
		candidate.GetNetworks(),
		acknowledged.GetNetworks(),
		references["networks"],
	)
	if err != nil {
		return nil, invalidCandidateRuntimeProxySource()
	}
	hybrid.Volumes, err = attachRuntimeVolumes(candidate.GetVolumes(), acknowledged.GetVolumes(), references["volumes"])
	if err != nil {
		return nil, invalidCandidateRuntimeProxySource()
	}
	return hybrid, nil
}

func retainCandidateRuntimeProxyYAML(
	candidate *agentpb.ComposeArtifact,
	workloadName string,
	acknowledged *agentpb.ComposeArtifact,
	proxyName string,
) ([]byte, map[string]map[string]bool, error) {
	var candidateDocument, acknowledgedDocument yaml.Node
	if yaml.Unmarshal(candidate.GetCanonicalYaml(), &candidateDocument) != nil ||
		yaml.Unmarshal(acknowledged.GetCanonicalYaml(), &acknowledgedDocument) != nil ||
		len(candidateDocument.Content) != 1 || len(acknowledgedDocument.Content) != 1 {
		return nil, nil, invalidCandidateRuntimeProxySource()
	}
	candidateRoot, acknowledgedRoot := candidateDocument.Content[0], acknowledgedDocument.Content[0]
	workload, err := attachRuntimeYAMLService(candidateRoot, workloadName)
	if err != nil {
		return nil, nil, invalidCandidateRuntimeProxySource()
	}
	proxy, err := attachRuntimeYAMLService(acknowledgedRoot, proxyName)
	if err != nil {
		return nil, nil, invalidCandidateRuntimeProxySource()
	}
	candidateServices := candidateRuntimeMappingValue(candidateRoot, "services")
	proxyIndex, err := attachRuntimeYAMLMappingIndex(candidateServices, proxyName)
	if err != nil || proxyIndex < 0 {
		return nil, nil, invalidCandidateRuntimeProxySource()
	}

	workloadReferences := make(map[string]map[string]bool, 4)
	proxyReferences := make(map[string]map[string]bool, 4)
	if candidateRuntimeResourceReferences(workload, workloadReferences) != nil ||
		candidateRuntimeResourceReferences(proxy, proxyReferences) != nil {
		return nil, nil, invalidCandidateRuntimeProxySource()
	}
	for _, section := range []string{"networks", "volumes", "configs", "secrets"} {
		for _, name := range attachRuntimeSortedNames(proxyReferences[section]) {
			if workloadReferences[section][name] {
				before := candidateRuntimeMappingValue(candidateRuntimeMappingValue(candidateRoot, section), name)
				after := candidateRuntimeMappingValue(candidateRuntimeMappingValue(acknowledgedRoot, section), name)
				if !attachRuntimeYAMLEqual(before, after) ||
					!attachRuntimeSharedMetadataEqual(candidate, acknowledged, section, name) {
					return nil, nil, invalidCandidateRuntimeProxySource()
				}
				continue
			}
			if replaceErr := attachRuntimeReplaceYAMLResource(
				candidateRoot,
				acknowledgedRoot,
				section,
				name,
			); replaceErr != nil {
				return nil, nil, invalidCandidateRuntimeProxySource()
			}
		}
	}
	candidateServices.Content[proxyIndex+1] = proxy
	encoded, err := yaml.Marshal(&candidateDocument)
	if err != nil {
		return nil, nil, errs.Wrap(errs.KindInternal, err)
	}
	return encoded, proxyReferences, nil
}

func candidateRuntimeSingleService(values []string, serviceID string) bool {
	return len(values) == 1 && values[0] == serviceID
}

func candidateRuntimeOppositeSlots(left, right string) bool {
	return left == "blue" && right == "green" || left == "green" && right == "blue"
}

func marshalCandidateRuntimeArtifact(artifact *agentpb.ComposeArtifact) ([]byte, error) {
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil || len(encoded) == 0 || len(encoded) > MaximumPlanBytes {
		return nil, errs.New(errs.KindValidationFailed, "candidate runtime artifact encoding is invalid")
	}
	return encoded, nil
}

func invalidCandidateRuntimeForwardSteps() error {
	return errs.New(errs.KindValidationFailed, "candidate runtime forward steps do not bind one exact activation")
}

func invalidCandidateRuntimeProxySource() error {
	return errs.New(errs.KindValidationFailed, "candidate runtime existing proxy source is invalid")
}
