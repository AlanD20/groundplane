package coredns

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestDecodeConfigRequiresExactShape(t *testing.T) {
	t.Parallel()
	valid := map[string]any{
		"upstream_auto":      true,
		"upstream_resolvers": []any{},
		"forwarders":         []any{},
		"tailnet_delegation": false,
	}
	config, err := DecodeConfig(valid)
	if err != nil {
		t.Fatalf("DecodeConfig() error = %v", err)
	}
	if !config.UpstreamAuto || len(config.Forwarders) != 0 {
		t.Fatalf("DecodeConfig() = %+v", config)
	}
	for name, mutate := range map[string]func(map[string]any){
		"missing key":     func(value map[string]any) { delete(value, "forwarders") },
		"unknown key":     func(value map[string]any) { value["typo"] = false },
		"wrong bool type": func(value map[string]any) { value["upstream_auto"] = "true" },
	} {
		mutate := mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			candidate := make(map[string]any, len(valid)+1)
			for key, value := range valid {
				candidate[key] = value
			}
			mutate(candidate)
			if !errors.Is(ValidateConfig(candidate), errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("ValidateConfig(%s) did not return validation.failed", name)
			}
		})
	}
}

func TestDecodeConfigParsesResolversAndRejectsTailnetConflict(t *testing.T) {
	t.Parallel()
	config, err := DecodeConfig(map[string]any{
		"upstream_auto": false,
		"upstream_resolvers": []any{
			"8.8.8.8",
			"[2001:4860:4860::8888]:853",
			map[string]any{"address": "127.0.0.53", "port": 53},
		},
		"forwarders": []any{map[string]any{
			"domain":    "home.arpa",
			"resolvers": []any{"192.168.1.1"},
		}},
		"tailnet_delegation": false,
	})
	if err != nil {
		t.Fatalf("DecodeConfig() error = %v", err)
	}
	if len(config.UpstreamResolvers) != 3 || config.UpstreamResolvers[0].Address != netip.MustParseAddr("8.8.8.8") {
		t.Fatalf("decoded resolvers = %+v", config.UpstreamResolvers)
	}
	conflicting := config.ToMap()
	conflicting["tailnet_delegation"] = true
	conflicting["forwarders"] = append(conflicting["forwarders"].([]any), map[string]any{
		"domain": "ts.net", "resolvers": []any{"100.100.100.100"},
	})
	if !errors.Is(ValidateConfig(conflicting), errs.New(errs.KindValidationFailed, "")) {
		t.Fatal("ValidateConfig() accepted operator ts.net while tailnet delegation is enabled")
	}
}
