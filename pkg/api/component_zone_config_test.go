package api

import (
	"encoding/json"
	"testing"
)

// QA: CMP-01, HTTP-07, HTTP-08 - L0 response-model JSON only; no Zone ownership,
// network membership, primary address, Tunnel routing, or runtime effect is proved.
// Rationale: both integrations accept the same explicit ordered membership
// decision, while Tunnel credentials remain distinct from router settings.
func TestComponentConfigRoundTripsMultipleZones(t *testing.T) {
	zones := `["net_01ARZ3NDEKTSV4RRFFQ69G5FAX","net_01ARZ3NDEKTSV4RRFFQ69G5FAW"]`
	for _, body := range []string{
		`{"zone_ids":` + zones + `}`,
		`{"zone_ids":` + zones + `,"secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`,
	} {
		t.Run(body, func(t *testing.T) {
			var config ComponentConfig
			if err := json.Unmarshal([]byte(body), &config); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(config)
			if err != nil || string(encoded) != body {
				t.Fatalf("ordered membership roundtrip = %s, %v; want %s", encoded, err, body)
			}
		})
	}
}

// QA: CMP-01, HTTP-07, HTTP-08 - L0 mutation-model JSON only; no Secret lookup,
// Zone validation, config publication, or Component reconciliation is exercised.
// Rationale: the write-only credential union must not confuse shared zone_ids
// with the Caddy discriminator or discard explicitly selected secondary Zones.
func TestComponentConfigMutationRoundTripsMultipleZones(t *testing.T) {
	zones := `["net_01ARZ3NDEKTSV4RRFFQ69G5FAX","net_01ARZ3NDEKTSV4RRFFQ69G5FAW"]`
	for _, body := range []string{
		`{"zone_ids":` + zones + `}`,
		`{"zone_ids":` + zones + `,"credential":{"mode":"existing","secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"}}`,
	} {
		var input ComponentConfigMutationInput
		if err := json.Unmarshal([]byte(body), &input); err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(input)
		if err != nil || string(encoded) != body {
			t.Fatalf("ordered membership roundtrip = %s, %v; want %s", encoded, err, body)
		}
	}
}

// QA: CMP-01, UI-03 - L0 response-model rejection only; no same-Environment
// lookup, durable config preservation, or running Component is exercised.
// Rationale: explicit placement rejects incomplete, duplicated, malformed and
// superseded singular inputs instead of silently attaching a default network.
func TestComponentZoneConfigurationRejectsInvalidSelections(t *testing.T) {
	for _, body := range []string{
		`{"zone_id":"net_01ARZ3NDEKTSV4RRFFQ69G5FAX"}`,
		`{"zone_ids":null}`,
		`{"zone_ids":[]}`,
		`{"zone_ids":["invalid"]}`,
		`{"zone_ids":[null]}`,
		`{"zone_ids":["net_01ARZ3NDEKTSV4RRFFQ69G5FAX","net_01ARZ3NDEKTSV4RRFFQ69G5FAX"]}`,
		`{"secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`,
	} {
		var config ComponentConfig
		if err := json.Unmarshal([]byte(body), &config); err == nil {
			t.Fatalf("accepted invalid config %s", body)
		}
	}
}
