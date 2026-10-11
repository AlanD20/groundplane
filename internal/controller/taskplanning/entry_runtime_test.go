package taskplanning

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: replacing a captured proxy must not grant permission to replace
// authored or shared config, and must not mutate the immutable source artifact.
func TestEntryRuntimeReplacesOnlyOwnedProxyConfig(t *testing.T) {
	proxyConfig, err := serviceproxy.Render(serviceproxy.Configuration{
		Implementation: serviceproxy.Caddy,
		ReleaseID:      "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Generation:     7,
		Ports:          []serviceproxy.Port{{Number: 8080, Protocol: serviceproxy.TCP}},
		Upstreams:      []string{"api--blue"},
	})
	if err != nil {
		t.Fatal(err)
	}
	contentLine := []byte(fmt.Sprintf("content: %q", string(proxyConfig)))
	baseline := &agentpb.ComposeArtifact{
		CanonicalYaml: []byte("services:\n  api:\n    configs:\n      - source: gp-proxy-native\n" +
			"configs:\n  gp-proxy-native:\n    " + string(contentLine) + "\n  authored:\n    content: keep\n"),
		Services: []*agentpb.ComposeService{{ServiceId: "native", ComposeName: "api",
			Role:                agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			ProxyImplementation: agentpb.ServiceProxyImplementation_SERVICE_PROXY_IMPLEMENTATION_CADDY,
			ProxyConfigJson:     proxyConfig}},
	}
	digest := sha256.Sum256(baseline.Services[0].ProxyConfigJson)
	baseline.Services[0].ProxyConfigSha256 = digest[:]
	sources := []*agentpb.ComposeArtifact{baseline}
	prepared, err := entryRuntimeWithoutReplacedProxyConfigs(baseline, sources)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(prepared.CanonicalYaml, contentLine) ||
		!bytes.Contains(prepared.CanonicalYaml, []byte("content: keep")) ||
		!bytes.Contains(baseline.CanonicalYaml, contentLine) {
		t.Fatal("proxy replacement changed authored config or immutable source")
	}
	for _, mutation := range []string{"shared", "content", "component"} {
		t.Run(mutation, func(t *testing.T) {
			changed := proto.CloneOf(baseline)
			switch mutation {
			case "shared":
				changed.CanonicalYaml = bytes.Replace(changed.CanonicalYaml, []byte("services:\n"),
					[]byte("services:\n  other:\n    configs:\n      - source: gp-proxy-native\n"), 1)
			case "content":
				changed.CanonicalYaml = bytes.Replace(changed.CanonicalYaml, contentLine,
					[]byte("content: foreign"), 1)
			case "component":
				changed.Services[0].OwnerComponentId = "component"
			}
			_, err := entryRuntimeWithoutReplacedProxyConfigs(changed, sources)
			if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
				t.Fatalf("accepted %s proxy config: %v", mutation, err)
			}
		})
	}
}
