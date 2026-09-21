package app

import (
	"testing"

	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func mustProjectTaskOwner(t *testing.T, project testhierarchy.ProjectRecord) testtaskjournal.TaskOwner {
	t.Helper()
	owner, err := testtaskjournal.ProjectTaskOwner(project)
	if err != nil {
		t.Fatalf("ProjectTaskOwner() error = %v", err)
	}
	return owner
}
