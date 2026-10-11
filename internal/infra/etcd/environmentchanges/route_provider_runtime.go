package environmentchanges

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// DecodeRouteProviderRuntime opens only the captured acknowledged artifact.
// It cannot reconstruct a predecessor from current desired state or a live
// container. A missing acknowledgement remains distinct from runtime absence.
func DecodeRouteProviderRuntime(provider *RouteProviderPin) (*agentpb.ComposeArtifact, error) {
	if provider == nil || provider.Runtime == nil {
		return nil, errs.New(errs.KindValidationFailed, "Route has no captured router runtime")
	}
	retained := provider.Runtime
	if ids.Validate(ids.KindEnvironment, retained.EnvironmentID) != nil || retained.Revision < 0 ||
		retained.Revision > provider.InputRevision ||
		len(retained.Artifact) > environmentprojection.EnvironmentBlueprintProjectionMaxBytes {
		return nil, errs.New(errs.KindValidationFailed, "Route captured router runtime scope is invalid")
	}
	if len(retained.Artifact) == 0 {
		return nil, nil
	}
	if retained.Revision == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Route router artifact has no acknowledgement revision")
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(retained.Artifact, artifact); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Route captured router artifact is unreadable")
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	digest := sha256.Sum256(artifact.GetCanonicalYaml())
	if err != nil || !bytes.Equal(canonical, retained.Artifact) ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != retained.EnvironmentID ||
		ids.Validate(ids.KindConfig, artifact.GetArtifactId()) != nil ||
		len(artifact.GetCanonicalYaml()) == 0 || !bytes.Equal(digest[:], artifact.GetYamlSha256()) {
		return nil, errs.New(errs.KindValidationFailed, "Route captured router artifact identity is invalid")
	}
	matched := 0
	for _, service := range artifact.GetServices() {
		if service.GetOwnerComponentId() != provider.ComponentID {
			continue
		}
		if service.GetServiceId() != provider.ServiceID {
			return nil, errs.New(errs.KindValidationFailed, "Route captured router artifact differs from its provider")
		}
		matched++
	}
	if matched != 1 {
		return nil, errs.New(errs.KindValidationFailed, "Route captured router Service is missing or ambiguous")
	}
	return artifact, nil
}
