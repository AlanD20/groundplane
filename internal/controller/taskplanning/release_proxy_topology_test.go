package taskplanning

import (
	"fmt"
	"testing"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// SVC-15/BP-04: switching a blue-green destination cannot silently replace a
// proxy's network attachments or listening sockets and interrupt existing traffic.
func TestBlueGreenRejectsDisruptiveProxyTopologyChanges(t *testing.T) {
	artifact := func(port uint32, alias, restart string) *agentpb.ComposeArtifact {
		return &agentpb.ComposeArtifact{
			Services: []*agentpb.ComposeService{{ServiceId: "api", ComposeName: "proxy",
				Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY}},
			CanonicalYaml: []byte(
				fmt.Sprintf(
					"services:\n  proxy:\n    networks:\n      app:\n        aliases: [%s]\n    ports:\n      - target: %d\n        published: '8080'\n        protocol: tcp\n    restart: %s\n",
					alias,
					port,
					restart,
				),
			),
		}
	}
	prior := artifact(80, "api", "always")
	for _, candidate := range []*agentpb.ComposeArtifact{
		artifact(81, "api", "always"), artifact(80, "changed", "always"), artifact(80, "api", "unless-stopped"),
	} {
		if err := validateReleaseProxyTopology(domain.StrategyBlueGreen, candidate, prior, "api"); err == nil {
			t.Fatal("blue-green accepted a proxy-recreating change")
		}
		if err := validateReleaseProxyTopology(domain.StrategyRecreate, candidate, prior, "api"); err != nil {
			t.Fatal(err)
		}
	}
	if err := validateReleaseProxyTopology(domain.StrategyBlueGreen, artifact(80, "api", "always"), prior, "api"); err != nil {
		t.Fatal(err)
	}
}
