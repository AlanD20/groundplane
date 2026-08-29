package component

import (
	"crypto/sha256"
	"testing"
)

// Rationale: catalog identity must stay independent of source declaration
// order while changing whenever an executable action contract changes.
func TestDefinitionDigestIncludesCanonicalActions(t *testing.T) {
	activate, err := NewActionDefinition("activate-config", CapabilityManagedConfig, OperationActivate)
	if err != nil {
		t.Fatalf("NewActionDefinition() error = %v", err)
	}
	observe, err := NewActionDefinition("observe-config", CapabilityManagedConfig, OperationObserve)
	if err != nil {
		t.Fatalf("NewActionDefinition() error = %v", err)
	}

	first := mustDefinition(t, []ActionDefinition{observe, activate})
	second := mustDefinition(t, []ActionDefinition{activate, observe})
	if first.Digest() != second.Digest() {
		t.Fatal("Definition digest depends on action declaration order")
	}

	changed := mustDefinition(t, []ActionDefinition{activate})
	if first.Digest() == changed.Digest() {
		t.Fatal("Definition digest ignores action declarations")
	}
}

// Rationale: a Task action must carry only sealed catalog and artifact
// identities so Agent execution cannot recover commands from untrusted input.
func TestActionEnvelopeRequiresSealedIdentity(t *testing.T) {
	componentID, err := NewComponentID("cmp_01M0ZJAQH5YE4KVB81DW6G0M3W")
	if err != nil {
		t.Fatalf("NewComponentID() error = %v", err)
	}
	artifactID, err := NewArtifactID("artifact_01M0ZJAQH5YE4KVB81DW6G0M3W")
	if err != nil {
		t.Fatalf("NewArtifactID() error = %v", err)
	}
	digest := sha256.Sum256([]byte("sealed"))
	artifact, err := NewArtifactReference(artifactID, digest)
	if err != nil {
		t.Fatalf("NewArtifactReference() error = %v", err)
	}
	envelope, err := NewActionEnvelope(ActionEnvelopeInput{
		ComponentID: componentID, DefinitionDigest: digest, CatalogDigest: digest,
		ActionID: "activate-config", Artifact: artifact, Generation: 1,
	})
	if err != nil {
		t.Fatalf("NewActionEnvelope() error = %v", err)
	}
	if envelope.ComponentID() != componentID || envelope.ActionID() != "activate-config" ||
		envelope.Artifact().ID() != artifactID || envelope.Generation() != 1 {
		t.Fatal("ActionEnvelope changed sealed identity")
	}
}

func mustDefinition(t *testing.T, actions []ActionDefinition) Definition {
	t.Helper()
	definition, err := NewDefinition(DefinitionInput{
		Implementation: "test-router",
		ConfigVariant:   "test-router-v1",
		Provides:        []Capability{CapabilityHTTPRouter},
		OwnerScopes:     []OwnerScope{OwnerScopeEnvironment},
		Actions:         actions,
	})
	if err != nil {
		t.Fatalf("NewDefinition() error = %v", err)
	}
	return definition
}
