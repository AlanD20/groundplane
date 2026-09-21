package app

import (
	"testing"

	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func mustEnvironmentTaskOwner(
	t *testing.T,
	project testhierarchy.ProjectRecord,
	environment testhierarchy.EnvironmentRecord,
) testtaskjournal.TaskOwner {
	t.Helper()
	owner, err := testtaskjournal.EnvironmentTaskOwner(project, environment)
	if err != nil {
		t.Fatalf("EnvironmentTaskOwner() error = %v", err)
	}
	return owner
}
