package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: terminal Component reconciliation must promote opaque secret
// references generically, without persisting provider-specific credential data.
func TestComponentTaskTerminalSecretMutationsPromoteReference(t *testing.T) {
	componentID := "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	currentSecret := "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	nextSecret := "sec_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	taskID := "tsk_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	current := ComponentRecord{Desired: ComponentDesiredRecord{
		ID:     componentID,
		Config: core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{SecretID: currentSecret}},
	}}
	next := ComponentRecord{Desired: ComponentDesiredRecord{
		ID:     componentID,
		Config: core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{SecretID: nextSecret}},
	}}
	mutations, err := componentTaskTerminalSecretMutations(ComponentTaskIntent{
		Candidates: []ComponentTaskCandidate{{Current: current, Candidate: next}},
	}, taskID, TaskStatusCompleted)
	if err != nil {
		t.Fatalf("componentTaskTerminalSecretMutations() error = %v", err)
	}
	if len(mutations) != 3 ||
		mutations[0].Key != componentCandidateSecretReferenceKey(nextSecret, taskID, componentID) ||
		mutations[1].Key != componentActiveSecretReferenceKey(currentSecret, componentID) ||
		mutations[2].Key != componentActiveSecretReferenceKey(nextSecret, componentID) {
		t.Fatalf("componentTaskTerminalSecretMutations() = %#v", mutations)
	}
}
