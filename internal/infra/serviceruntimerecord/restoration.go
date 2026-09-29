package serviceruntimerecord

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Restore acknowledges exact predecessor bytes after the caller has validated
// the sealed restoration proof. It never rerenders the predecessor's YAML.
func Restore(
	environmentID, serviceID, releaseID, target string,
	current, retained []byte,
	source Acknowledgement,
) (Record, error) {
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(current, artifact) != nil {
		return Record{}, invalidRuntimeIdentity()
	}
	runtime := executionplan.CandidateRuntime{ServiceID: serviceID, ReleaseID: releaseID, Target: target,
		CurrentArtifact: bytes.Clone(current), RetainedPriorArtifact: bytes.Clone(retained)}
	for _, service := range artifact.Services {
		if service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		generation, err := executionplan.ProxyConfigGeneration(service.ProxyConfigJson, releaseID)
		if err != nil {
			return Record{}, err
		}
		runtime.ProxyGeneration, runtime.ProxyConfigSHA256 = generation, bytes.Clone(service.ProxyConfigSha256)
	}
	record := Record{EnvironmentID: environmentID, Runtime: runtime, Source: source}
	if err := Validate(record); err != nil {
		return Record{}, err
	}
	return record, nil
}
