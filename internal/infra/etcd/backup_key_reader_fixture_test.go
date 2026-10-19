package etcd

import (
	"testing"

	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
)

func backupPolicyKeyReader(t *testing.T, store *memoryHierarchyStore) *testbackuppolicy.KeyRepository {
	t.Helper()
	repository, err := testbackuppolicy.NewKeyRepository(&releaseLogMemoryStore{store})
	if err != nil {
		t.Fatal(err)
	}
	return repository
}
