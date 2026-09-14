package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/runtimeconfiguration"
)

func (fixture *ExecutedArtifactFixture) ConfigurationSources(t *testing.T) *runtimeconfiguration.Repository {
	t.Helper()
	repository, err := NewRuntimeConfigurationRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	return repository
}
