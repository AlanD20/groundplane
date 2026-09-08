package etcd

import (
	"testing"

	domain "github.com/AlanD20/groundplane/internal/core/release"
)

func TestRollbackIntentMatchesBothIndexedOwners(t *testing.T) {
	view := ReleaseView{
		Intent: domain.Intent{
			EnvironmentID: "env_01J00000000000000000000000",
			ServiceID:     "svc_01J00000000000000000000000",
		},
	}
	if !rollbackIntentMatches(view, view.Intent.EnvironmentID, view.Intent.ServiceID) {
		t.Fatal("matching rollback intent rejected")
	}
	if rollbackIntentMatches(view, "env_01J00000000000000000000001", view.Intent.ServiceID) {
		t.Fatal("wrong Environment rollback intent accepted")
	}
	if rollbackIntentMatches(view, view.Intent.EnvironmentID, "svc_01J00000000000000000000001") {
		t.Fatal("wrong Service rollback intent accepted")
	}
}
