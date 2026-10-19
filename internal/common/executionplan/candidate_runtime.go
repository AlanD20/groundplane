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
// for one captured native Release. It is not acknowledgement
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
