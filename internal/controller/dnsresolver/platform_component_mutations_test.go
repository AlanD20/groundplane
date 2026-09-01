package dnsresolver

import (
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: the strict typed DNS-resolver config must preserve canonical
// resolver ordering and address/port identity before durable publication.
func TestCoreDNSConfigMutationCanonicalizesResolvers(t *testing.T) {
	upstreamAuto := false
	tailnetDelegation := true
	corefileTemplate := testCorefileTemplate
	upstreamResolvers := []string{"1.1.1.1:853", "8.8.8.8"}
	forwarders := []apiTypes.ComponentDNSForwarder{{
		Domain: "home.arpa", Resolvers: []string{"192.168.1.1:5353", "192.168.1.2"},
	}}
	config, err := coreDNSConfigMutation(apiTypes.ComponentConfigMutationInput{
		CoreDNS: &apiTypes.CoreDNSComponentConfigMutationInput{
			CorefileTemplate: &corefileTemplate,
			UpstreamAuto:     &upstreamAuto, UpstreamResolvers: &upstreamResolvers,
			Forwarders: &forwarders, TailnetDelegation: &tailnetDelegation,
		},
	})
	if err != nil {
		t.Fatalf("coreDNSConfigMutation() error = %v", err)
	}
	if config.UpstreamResolvers[0].Address != "1.1.1.1" || config.UpstreamResolvers[0].Port != 853 ||
		config.UpstreamResolvers[1].Address != "8.8.8.8" || config.Forwarders[0].Resolvers[0].Port != 5353 {
		t.Fatalf("coreDNSConfigMutation() = %+v", config)
	}
}

// Rationale: no flattened or partial compatibility shape may bypass the
// closed nullable Component config union.
func TestCoreDNSConfigMutationRejectsPartialConfig(t *testing.T) {
	upstreamAuto := true
	if _, err := coreDNSConfigMutation(apiTypes.ComponentConfigMutationInput{
		CoreDNS: &apiTypes.CoreDNSComponentConfigMutationInput{UpstreamAuto: &upstreamAuto},
	}); err == nil {
		t.Fatal("coreDNSConfigMutation() accepted a partial config")
	}
}
