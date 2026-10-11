package executionplan

import (
	"bytes"
	"crypto/sha256"
	"slices"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func openRollingBlueprintRuntime(
	artifact *agentpb.ComposeArtifact,
	input BlueprintRuntimeInput,
) (CandidateRuntime, error) {
	if len(input.RetainedPriorArtifact) != 0 ||
		ValidateNativePredecessorWitness(artifact.GetOwnerId(), input.ServiceID, input.ActivatedArtifact, nil) != nil {
		return CandidateRuntime{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint rolling runtime has an invalid native replica set",
		)
	}
	workloads, proxy, err := nativePredecessorServices(artifact, input.ServiceID)
	if err != nil || len(workloads) == 0 ||
		workloads[0].GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA ||
		nativePredecessorReleaseID(workloads[0]) != input.ReleaseID {
		return CandidateRuntime{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint rolling runtime belongs to another Release",
		)
	}
	yamlDigest := sha256.Sum256(artifact.GetCanonicalYaml())
	if !bytes.Equal(yamlDigest[:], artifact.GetYamlSha256()) {
		return CandidateRuntime{}, errs.New(errs.KindValidationFailed, "Blueprint rolling runtime YAML digest differs")
	}
	result := CandidateRuntime{ServiceID: input.ServiceID, ReleaseID: input.ReleaseID, Target: "rolling",
		CurrentArtifact: slices.Clone(input.ActivatedArtifact)}
	if proxy != nil {
		generation, err := ProxyConfigGeneration(
			proxy.GetProxyImplementation(),
			proxy.GetProxyConfigJson(),
			input.ReleaseID,
		)
		digest := sha256.Sum256(proxy.GetProxyConfigJson())
		if err != nil || !bytes.Equal(digest[:], proxy.GetProxyConfigSha256()) {
			return CandidateRuntime{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint rolling proxy configuration differs from its Release",
			)
		}
		result.ProxyGeneration, result.ProxyConfigSHA256 = generation, slices.Clone(digest[:])
	}
	return result, nil
}
