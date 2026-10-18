package releaserender

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	domain "github.com/AlanD20/groundplane/internal/core/release"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Recovery uses the captured serving configuration, not a reconstruction from
// the candidate's name, ports or ledger revision.
func PriorProxyConfiguration(
	member ReleaseTaskRenderMember,
	artifact *agentpb.ComposeArtifact,
) (domain.ProxyConfig, error) {
	var selected *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != member.Render.ServiceID ||
			service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		if selected != nil {
			return domain.ProxyConfig{}, errs.New(errs.KindStateConflict, "native predecessor proxy is ambiguous")
		}
		selected = service
	}
	if selected == nil {
		return domain.ProxyConfig{}, errs.New(errs.KindStateConflict, "native predecessor proxy is missing")
	}
	digest := sha256.Sum256(selected.ProxyConfigJson)
	generation, err := executionplan.ProxyConfigGeneration(
		selected.GetProxyImplementation(),
		selected.ProxyConfigJson,
		member.Intent.PriorServingReleaseID,
	)
	if err != nil || generation != member.Render.PriorProxyGeneration ||
		!bytes.Equal(digest[:], selected.ProxyConfigSha256) ||
		hex.EncodeToString(digest[:]) != member.Render.PriorProxyDigest {
		return domain.ProxyConfig{}, errs.New(
			errs.KindStateConflict,
			"release rollback proxy differs from native predecessor",
		)
	}
	implementation, err := executionplan.OpenProxyImplementation(selected.GetProxyImplementation())
	if err != nil {
		return domain.ProxyConfig{}, err
	}
	return domain.ProxyConfig{
		Implementation: implementation,
		JSON:           slices.Clone(selected.ProxyConfigJson),
		SHA256:         digest,
	}, nil
}

// A rolling predecessor cannot be reconstructed from one workload name.
// Preserve its exact serving proxy config while the blue-green slot starts.
func InitialProxyConfiguration(member ReleaseTaskRenderMember) (*domain.ProxyConfig, error) {
	if member.Render.Strategy != domain.StrategyBlueGreen || member.Render.PriorTarget != domain.WorkloadRolling {
		return nil, nil
	}
	if member.Render.PriorRuntime == nil {
		return nil, errs.New(
			errs.KindStateConflict,
			"rolling-to-blue-green requires the acknowledged predecessor runtime",
		)
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(member.Render.PriorRuntime.CurrentArtifact, artifact); err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	config, err := PriorProxyConfiguration(member, artifact)
	if err != nil {
		return nil, err
	}
	return &config, nil
}
