package componentplanning

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: terminal Component reconciliation must promote opaque secret
// references generically, without persisting provider-specific credential data.
func TestComponentTaskTerminalSecretMutationsPromoteReference(t *testing.T) {
	componentID := "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	currentSecret := "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	nextSecret := "sec_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	taskID := "tsk_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	current := testcomponents.Record{Desired: testcomponents.DesiredRecord{
		ID:     componentID,
		Config: core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{SecretID: currentSecret}},
	}}
	next := testcomponents.Record{Desired: testcomponents.DesiredRecord{
		ID:     componentID,
		Config: core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{SecretID: nextSecret}},
	}}
	mutations, err := ComponentTaskTerminalSecretMutations(testenvironmentchanges.ComponentTaskIntent{
		Candidates: []testenvironmentchanges.ComponentTaskCandidate{{Current: current, Candidate: next}},
	}, taskID, testtaskjournal.TaskStatusCompleted)
	if err != nil {
		t.Fatalf("ComponentTaskTerminalSecretMutations() error = %v", err)
	}
	if len(mutations) != 3 ||
		mutations[0].Key != componentCandidateSecretReferenceKey(nextSecret, taskID, componentID) ||
		mutations[1].Key != componentActiveSecretReferenceKey(currentSecret, componentID) ||
		mutations[2].Key != componentActiveSecretReferenceKey(nextSecret, componentID) {
		t.Fatalf("ComponentTaskTerminalSecretMutations() = %#v", mutations)
	}
}
