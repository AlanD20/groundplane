package app

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
)

func blueprintImageLookupAgent(t *testing.T, fixture *ExecutedArtifactFixture) *agentregistration.Repository {
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
