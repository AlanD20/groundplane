package etcd

import "testing"

func TestEnvironmentDirectoryTaskResultIsClosedAndCarriesNoComposeObservation(t *testing.T) {
	t.Parallel()
	valid := TaskResultRecord{
		Kind: TaskResultEnvironmentDirectory, Diagnostic: TaskResultDiagnosticNone,
	}
	if err := validateTaskResult(valid, nil, TaskStatusCompleted); err != nil {
		t.Fatalf("validateTaskResult() error = %v", err)
	}
	invalid := valid
	invalid.Projects = []TaskObservedProjectSummary{{ProjectName: "not-directory-state"}}
	if err := validateTaskResult(invalid, nil, TaskStatusCompleted); err == nil {
		t.Fatal("validateTaskResult() accepted Compose observations for a directory helper")
	}
}
