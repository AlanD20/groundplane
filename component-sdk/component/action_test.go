package component

import (
	"crypto/sha256"
	"testing"
)

// QA: CMP-04; pure SDK catalog identity only, not publication or Agent execution.
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

// QA: CMP-04; pure SDK envelope validation only, not catalog resolution or runtime effects.
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
	definitionDigest := sha256.Sum256([]byte("definition"))
	catalogDigest := sha256.Sum256([]byte("catalog"))
	artifactDigest := sha256.Sum256([]byte("artifact"))
	artifact, err := NewArtifactReference(artifactID, artifactDigest)
	if err != nil {
		t.Fatalf("NewArtifactReference() error = %v", err)
	}
	input := ActionEnvelopeInput{
		ComponentID: componentID, DefinitionDigest: definitionDigest, CatalogDigest: catalogDigest,
		ActionID: "activate-config", Artifact: artifact, Generation: 1,
	}
	envelope, err := NewActionEnvelope(input)
	if err != nil {
		t.Fatalf("NewActionEnvelope() error = %v", err)
	}
	if envelope.ComponentID() != componentID || envelope.ActionID() != "activate-config" ||
		envelope.DefinitionDigest() != definitionDigest || envelope.CatalogDigest() != catalogDigest ||
		envelope.Artifact().ID() != artifactID || envelope.Artifact().Digest() != artifactDigest ||
		envelope.Generation() != 1 {
		t.Fatal("ActionEnvelope changed sealed identity")
	}

	for name, mutate := range map[string]func(*ActionEnvelopeInput){
		"component":  func(candidate *ActionEnvelopeInput) { candidate.ComponentID = ComponentID{} },
		"definition": func(candidate *ActionEnvelopeInput) { candidate.DefinitionDigest = [sha256.Size]byte{} },
		"catalog":    func(candidate *ActionEnvelopeInput) { candidate.CatalogDigest = [sha256.Size]byte{} },
		"action":     func(candidate *ActionEnvelopeInput) { candidate.ActionID = "invalid action" },
		"artifact":   func(candidate *ActionEnvelopeInput) { candidate.Artifact = ArtifactReference{} },
		"generation": func(candidate *ActionEnvelopeInput) { candidate.Generation = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := input
			mutate(&candidate)
			if _, err := NewActionEnvelope(candidate); err == nil {
				t.Fatal("NewActionEnvelope() accepted incomplete execution authority")
			}
		})
	}
}

func mustDefinition(t *testing.T, actions []ActionDefinition) Definition {
	t.Helper()
	definition, err := NewDefinition(DefinitionInput{
		Implementation: "test-router",
		ConfigVariant:  "test-router-v1",
		Provides:       []Capability{CapabilityHTTPRouter},
		OwnerScopes:    []OwnerScope{OwnerScopeEnvironment},
		Actions:        actions,
	})
	if err != nil {
		t.Fatalf("NewDefinition() error = %v", err)
	}
	return definition
}
