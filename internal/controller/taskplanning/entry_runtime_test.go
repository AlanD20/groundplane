package taskplanning

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: replacing a captured proxy must not grant permission to replace
// authored or shared config, and must not mutate the immutable source artifact.
func TestEntryRuntimeReplacesOnlyOwnedProxyConfig(t *testing.T) {
	baseline := &agentpb.ComposeArtifact{
		CanonicalYaml: []byte("services:\n  api:\n    configs:\n      - source: gp-proxy-native\n" +
			"configs:\n  gp-proxy-native:\n    content: sealed\n  authored:\n    content: keep\n"),
		Services: []*agentpb.ComposeService{{ServiceId: "native", ComposeName: "api",
			Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY, ProxyConfigJson: []byte("sealed")}},
	}
	digest := sha256.Sum256(baseline.Services[0].ProxyConfigJson)
	baseline.Services[0].ProxyConfigSha256 = digest[:]
	sources := []*agentpb.ComposeArtifact{baseline}
	prepared, err := entryRuntimeWithoutReplacedProxyConfigs(baseline, sources)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(prepared.CanonicalYaml, []byte("content: sealed")) ||
		!bytes.Contains(prepared.CanonicalYaml, []byte("content: keep")) ||
		!bytes.Contains(baseline.CanonicalYaml, []byte("content: sealed")) {
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
				changed.CanonicalYaml = bytes.Replace(changed.CanonicalYaml, []byte("content: sealed"),
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
