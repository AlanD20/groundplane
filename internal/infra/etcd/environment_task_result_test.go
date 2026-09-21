package etcd

import (
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"testing"
)

func TestEnvironmentDirectoryTaskResultIsClosedAndCarriesNoComposeObservation(t *testing.T) {
	t.Parallel()
	valid := testtaskjournal.TaskResultRecord{
		Kind: testtaskjournal.TaskResultEnvironmentDirectory, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
	}
	if err := testtaskjournal.ValidateTaskResult(valid, nil, testtaskjournal.TaskStatusCompleted); err != nil {
		t.Fatalf("validateTaskResult() error = %v", err)
	}
	invalid := valid
	invalid.Projects = []testtaskjournal.TaskObservedProjectSummary{{ProjectName: "not-directory-state"}}
	if err := testtaskjournal.ValidateTaskResult(invalid, nil, testtaskjournal.TaskStatusCompleted); err == nil {
		t.Fatal("validateTaskResult() accepted Compose observations for a directory helper")
	}
}
