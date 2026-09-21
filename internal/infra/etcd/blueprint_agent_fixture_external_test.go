package etcd_test

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"testing"
)

func blueprintImageLookupAgent(t *testing.T, fixture *etcd.ExecutedArtifactFixture) *agentregistration.Repository {
	t.Helper()
	repository, err := agentregistration.NewRepository(fixture.ImageLookupStore())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.CreateSingleton(t.Context(), fixture.ImageLookupAgentRecord()); err != nil {
		t.Fatal(err)
	}
	return repository
}
