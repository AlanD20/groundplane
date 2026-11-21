package component

import (
	"context"
	"testing"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/pkg/api"
)

// Replaying a request accepted by the previous Controller must match its
// retained protected intent; selecting another credential must still differ.
func TestTunnelComponentIntentReplayAcrossEnvironmentCredentialExtension(t *testing.T) {
	protector, err := secretvalue.NewProtector(intentCompatibilityCipher{}, intentCompatibilityCipher{})
	if err != nil {
		t.Fatal(err)
	}
	protect := func(value requestidempotency.Value) secretvalue.Envelope {
		intent := requestidempotency.CanonicalIntentV1{
			Method: "PUT", Route: "/components/{id}/config",
			Scope: requestidempotency.Scope{
				Kind: requestidempotency.ScopeEnvironment,
				ID:   "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			},
			Path:  []requestidempotency.PathBinding{{Name: "id", Value: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"}},
			Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(value),
		}
		version, digest, err := requestidempotency.Canonicalize(t.Context(), intent)
		if err != nil {
			t.Fatal(err)
		}
		defer digest.Destroy()
		sealed, err := requestidempotency.Protect(t.Context(), protector, version, digest)
		if err != nil {
			t.Fatal(err)
		}
		return sealed
	}
	old := protect(requestidempotency.Object(
		requestidempotency.Field{Name: "implementation", Value: requestidempotency.String("cloudflare-tunnel")},
		requestidempotency.Field{
			Name:  "zone_ids",
			Value: componentStringList([]string{"net_01ARZ3NDEKTSV4RRFFQ69G5FAV"}),
		},
		requestidempotency.Field{Name: "secret_id", Value: requestidempotency.String("sec_01ARZ3NDEKTSV4RRFFQ69G5FAV")},
	))
	defer old.Clear()
	config := &api.ComponentConfig{CloudflareTunnel: &api.CloudflareTunnelComponentConfig{
		ZoneIDs: []string{"net_01ARZ3NDEKTSV4RRFFQ69G5FAV"}, SecretID: "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"}}
	for _, changed := range []bool{false, true} {
		if changed {
			config.CloudflareTunnel.SecretID, config.CloudflareTunnel.TokenEntry = "", "ENV_TOKEN"
		}
		value, err := componentMutationConfigValue(config)
		if err != nil {
			t.Fatal(err)
		}
		candidate := protect(value)
		matched, err := requestidempotency.CompareProtected(t.Context(), protector, old, candidate)
		candidate.Clear()
		if err != nil || matched == changed {
			t.Fatalf("legacy replay changed=%t matched=%t error=%v", changed, matched, err)
		}
	}
}

type intentCompatibilityCipher struct{}

func (intentCompatibilityCipher) Seal(_ context.Context, value []byte) ([]byte, error) {
	return append([]byte(nil), value...), nil
}
func (intentCompatibilityCipher) Open(_ context.Context, value []byte) ([]byte, error) {
	return append([]byte(nil), value...), nil
}
