package dnsresolver

import (
	"crypto/sha256"
	"encoding/json"
	"net/netip"
	"testing"

	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/core"
)

type rendererPin struct{}

func (rendererPin) Render(input componentdns.RenderInput) ([]byte, error) {
	encoded, err := json.Marshal(input)
	return encoded, err
}

func (rendererPin) Digest(input componentdns.RenderInput) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(input)
	if err != nil { return [sha256.Size]byte{}, err }
	return sha256.Sum256(encoded), nil
}

// Rationale: Controller planning must remain deterministic while consuming
// provider-neutral HTTP-router host observations.
func TestBuildTaskPlanIsDeterministic(t *testing.T) {
	t.Parallel()
	component := core.Component{
		ID: "cmp_coredns", Owner: core.ComponentOwnerPlatform, Kind: core.ComponentKindCoreDNS,
		Enabled: true, GeneratedServices: []string{"svc_coredns"},
		Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
			UpstreamAuto: true, TailnetDelegation: true,
		}},
	}
	hosts := []componentdns.Host{{Address: netip.MustParseAddr("10.200.30.4"), Hostnames: []string{"app.example.com"}}}
	baseline := []componentdns.ResolverEndpoint{{Address: netip.MustParseAddr("1.1.1.1")}}
	first, err := BuildTaskPlan(rendererPin{}, component, hosts, baseline)
	if err != nil { t.Fatalf("BuildTaskPlan() error = %v", err) }
	second, err := BuildTaskPlan(rendererPin{}, component, hosts, baseline)
	if err != nil { t.Fatalf("BuildTaskPlan() second error = %v", err) }
	if string(first.Corefile) != string(second.Corefile) || first.InputSHA256 != second.InputSHA256 || first.CorefileSHA256 != second.CorefileSHA256 {
		t.Fatal("BuildTaskPlan() was not deterministic")
	}
	if err := first.Validate(); err != nil { t.Fatalf("TaskPlan.Validate() error = %v", err) }
}
