package controller

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Recovery uses the captured serving configuration, not a reconstruction from
// the candidate's name, ports or ledger revision.
func releasePriorProxyConfig(
	member etcd.ReleaseTaskRenderMember,
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
	return domain.ProxyConfig{JSON: slices.Clone(selected.ProxyConfigJson), SHA256: digest}, nil
}
