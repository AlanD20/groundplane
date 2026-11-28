package resolverwork

import (
	"bytes"
	"crypto/sha256"
	"testing"

	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Upgrading the rendering code must preserve both the reconciliation comparison
// and a retained request's duplicate-suppression key until DNS intent changes.
func TestServingProofAloneDoesNotScheduleResolverWork(t *testing.T) {
	t.Parallel()
	capture := Capture{Desired: componentrecord.DesiredRecord{
		ID: ids.New(
			ids.KindComponent,
		), Owner: core.ComponentOwnerPlatform, Kind: core.ComponentKindCoreDNS, Enabled: true,
		Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
			CorefileTemplate: ". {\n    {groundplane}\n    prometheus 127.0.0.1:9153\n    log\n    errors\n}\n",
			UpstreamAuto:     true,
		}},
	}}
	old := []byte(". {\n    bind 127.0.0.1\n    forward . 1.1.1.1\n    reload\n" +
		"    prometheus 127.0.0.1:9153\n    log\n    errors\n}\n")
	marked := func(content []byte) []byte {
		t.Helper()
		result, err := componentdns.AddCorefileServingProof(content)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	trigger := func(content []byte) string {
		t.Helper()
		digest := sha256.Sum256(content)
		input := resolverexecution.Input{Candidate: &resolverexecution.Configuration{
			Corefile: content, Apply: &agentpb.ComponentApply{ArtifactDigest: digest[:]},
			Runtime: &agentpb.ComposeArtifact{Services: []*agentpb.ComposeService{
				{ImageReference: "coredns/coredns"},
			}},
		}}
		result, err := fingerprint(input, capture, 42)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	baseline := trigger(old)
	if trigger(marked(old)) != baseline {
		t.Fatal("legacy and proof-enabled files would schedule different intents")
	}
	changed := bytes.Replace(old, []byte("1.1.1.1"), []byte("8.8.8.8"), 1)
	if trigger(marked(changed)) == baseline {
		t.Fatal("changed upstream would fail to schedule new resolver work")
	}
}
