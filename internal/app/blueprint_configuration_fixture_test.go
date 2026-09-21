package app

import (
	"testing"

	runtimeconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
)

func (fixture *ExecutedArtifactFixture) ConfigurationSources(t *testing.T) *runtimeconfiguration.Repository {
	t.Helper()
	repository, err := runtimeconfiguration.New(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	return repository
}
