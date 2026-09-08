package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Rationale: deploying an unrelated group must not erase a separately serving
// Service's restoration authority. The Environment artifact is not its history.
func TestOrdinaryClaimAfterUnrelatedEnvironmentApply(t *testing.T) {
	fixture, _, _, revision, _ := recoveryProofFixture(t, false)
	task := fixture.claim.Task.Record
	read, err := fixture.repository.store.GetMany(t.Context(), GetManyRequest{
		Keys:     []string{releaseRenderInputStagingKey(task.Params[TaskReleasePublicationParam], fixture.releaseID)},
		Revision: revision,
	})
	if err != nil || len(read.Values) != 1 || read.Values[0] == nil {
		t.Fatalf("read candidate render: %v", err)
	}
	render, err := decodeReleaseRecord[ReleaseRenderInput](read.Values[0].Value, "release-render-input")
	if err != nil {
		t.Fatal(err)
	}
	projection := render.Projection
	otherService := ids.NewAt(ids.KindService, fixture.now, 230)
	projection.DesiredServices[0].Desired.ID = otherService
	projection = withTestEnvironmentComposeArtifact(projection)
	encoded, err := encodeEnvironmentComposeProjection(projection)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := fixture.repository.store.Transact(t.Context(), nil, []Mutation{{
		Type: MutationPut, Key: environmentComposeProjectionKey(task.Owner.EnvironmentID), Value: encoded,
	}})
	if err != nil {
		t.Fatal(err)
	}
	authority, _, _, err := fixture.repository.prepareOrdinaryRestorationAuthority(t.Context(), task, changed.Revision)
	if err != nil {
		t.Fatalf("unrelated Environment apply erased serving predecessor: %v", err)
	}
	if len(authority.Candidates) != 1 || authority.Candidates[0].ServiceID != fixture.serviceID ||
		authority.Candidates[0].Target != ReleaseRestorationServingPredecessor {
		t.Fatal("claim lost independent serving Service")
	}
}
