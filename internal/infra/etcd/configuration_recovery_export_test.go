package etcd

import (
	runtimeconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"testing"
)

func (fixture *ExecutedArtifactFixture) ConfigurationSources(t *testing.T) *runtimeconfiguration.Repository {
	t.Helper()
	repository, err := runtimeconfiguration.New(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	return repository
}
