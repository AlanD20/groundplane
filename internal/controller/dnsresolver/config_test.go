package dnsresolver

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestDecodeConfigRequiresExactVariantAndMode(t *testing.T) {
	t.Parallel()
	valid := core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{UpstreamAuto: true}}
	config, err := DecodeConfig(valid)
	if err != nil {
		t.Fatalf("DecodeConfig() error = %v", err)
	}
	if !config.UpstreamAuto || len(config.Forwarders) != 0 {
		t.Fatalf("DecodeConfig() = %+v", config)
	}
	invalid := []core.ComponentConfig{
		{},
		{Caddy: &core.CaddyComponentConfig{}, CoreDNS: &core.CoreDNSComponentConfig{UpstreamAuto: true}},
		{CoreDNS: &core.CoreDNSComponentConfig{}},
	}
	for _, candidate := range invalid {
		if _, err := DecodeConfig(candidate); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Fatalf("DecodeConfig(%#v) error = %v, want validation.failed", candidate, err)
		}
	}
}

func TestDecodeConfigProjectsResolversAndRejectsTailnetConflict(t *testing.T) {
	t.Parallel()
	config, err := DecodeConfig(core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
		UpstreamResolvers: []core.DNSResolverEndpoint{
			{Address: "8.8.8.8"},
			{Address: "2001:4860:4860::8888", Port: 853},
			{Address: "127.0.0.53", Port: 53},
		},
		Forwarders: []core.DNSForwarder{{
			Domain: "home.arpa",
			Resolvers: []core.DNSResolverEndpoint{{Address: "192.168.1.1"}},
		}},
	}})
	if err != nil {
		t.Fatalf("DecodeConfig() error = %v", err)
	}
	if len(config.UpstreamResolvers) != 3 || config.UpstreamResolvers[0].Address != netip.MustParseAddr("8.8.8.8") {
		t.Fatalf("decoded resolvers = %+v", config.UpstreamResolvers)
	}
	_, err = DecodeConfig(core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
		UpstreamAuto: true,
		TailnetDelegation: true,
		Forwarders: []core.DNSForwarder{{
			Domain: "ts.net",
			Resolvers: []core.DNSResolverEndpoint{{Address: "100.100.100.100"}},
		}},
	}})
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("DecodeConfig(tailnet conflict) error = %v", err)
	}
}
