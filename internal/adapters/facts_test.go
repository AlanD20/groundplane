package adapters

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
)

func TestBuildFactsRendersTypedOwnAndGrantInputs(t *testing.T) {
	// Rationale: fact-derived Entries need exact adapter-owned keys and bytes;
	// URL/password outputs must remain marked secret and caller-clearable.
	adapter := factTestAdapter{schema: []FactDefinition{
		{Field: FactURL, Secret: true},
		{Field: FactHost},
		{Field: FactPort},
		{Field: FactDatabase},
		{Field: FactRole},
		{Field: FactPassword, Secret: true},
	}}
	facts, err := BuildFacts(adapter, FactParams{
		Host: "postgres", Port: "5432", Database: "api_5d3f9a", Role: "api_5d3f9a",
		Password: []byte("URL_safe-1"),
	})
	if err != nil {
		t.Fatalf("BuildFacts() error = %v", err)
	}
	if len(facts) != 6 || facts[0].Key != "pg16_URL" ||
		string(facts[0].Value) != "pgsql://api_5d3f9a:URL_safe-1@postgres:5432/api_5d3f9a" ||
		!facts[0].Secret || facts[1].Secret || !facts[5].Secret {
		t.Fatalf("BuildFacts() = %#v", facts)
	}
	values := make([][]byte, len(facts))
	for index := range facts {
		values[index] = facts[index].Value
	}
	ClearFacts(facts)
	for index, value := range values {
		for _, character := range value {
			if character != 0 {
				t.Fatalf("ClearFacts() retained fact %d bytes", index)
			}
		}
	}
}

func TestBuildFactsRejectsDuplicateSchemaAndKeepsManualFactless(t *testing.T) {
	// Rationale: ambiguous duplicate keys cannot form a durable fact map, while
	// the accepted manual adapter must remain a network-only no-facts operation.
	duplicate := factTestAdapter{schema: []FactDefinition{{Field: FactHost}, {Field: FactHost}}}
	if _, err := BuildFacts(duplicate, FactParams{
		Host: "postgres", Port: "5432", Role: "api", Password: []byte("safe"),
	}); err == nil {
		t.Fatal("BuildFacts() accepted duplicate fields")
	}
	manual := factTestAdapter{manual: true, prefix: "", scheme: ""}
	facts, err := BuildFacts(manual, FactParams{})
	if err != nil || len(facts) != 0 {
		t.Fatalf("BuildFacts(manual) = %#v, %v", facts, err)
	}
}

type factTestAdapter struct {
	schema []FactDefinition
	manual bool
	prefix string
	scheme string
}

func (adapter factTestAdapter) Key() string          { return "test" }
func (adapter factTestAdapter) Label() string        { return "Test" }
func (adapter factTestAdapter) DefaultImage() string { return "test:latest" }
func (adapter factTestAdapter) FactsPrefix() string {
	if adapter.prefix != "" || adapter.manual {
		return adapter.prefix
	}
	return "pg16_"
}
func (adapter factTestAdapter) URLScheme() string {
	if adapter.scheme != "" || adapter.manual {
		return adapter.scheme
	}
	return "pgsql://"
}
func (adapter factTestAdapter) Port() string {
	if adapter.manual {
		return ""
	}
	return "5432"
}
func (adapter factTestAdapter) FactSchema(core.BackingAuthentication) []FactDefinition {
	return adapter.schema
}
func (adapter factTestAdapter) SupportsAuthenticationModes() bool { return false }
func (adapter factTestAdapter) Manual() bool                      { return adapter.manual }
func (adapter factTestAdapter) SupportsGrants() bool              { return false }
func (adapter factTestAdapter) ProvisionSteps(ProvisionParams) []Step {
	return nil
}
func (adapter factTestAdapter) GrantSteps(ProvisionParams) []Step  { return nil }
func (adapter factTestAdapter) RevokeSteps(ProvisionParams) []Step { return nil }
func (adapter factTestAdapter) DetachSteps(ProvisionParams) []Step { return nil }
func (adapter factTestAdapter) BackupStrategy() BackupStrategy     { return BackupStrategy{} }
