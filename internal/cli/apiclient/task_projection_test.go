package apiclient

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: generated Task enums remain a transport detail; the handwritten
// projection must preserve backup_prune in the public string-valued Task model.
func TestTaskFromGeneratedPreservesBackupPruneType(t *testing.T) {
	t.Parallel()
	projected, err := taskFromGenerated(generated.Task{Type: generated.TaskTypeBackupPrune})
	if err != nil {
		t.Fatalf("taskFromGenerated() error = %v", err)
	}
	if projected.Type != "backup_prune" {
		t.Fatalf("taskFromGenerated().Type = %q, want backup_prune", projected.Type)
	}
}

func TestTaskFromGeneratedRejectsUnknownType(t *testing.T) {
	t.Parallel()

	_, err := taskFromGenerated(generated.Task{Type: generated.TaskType("future_task")})
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("taskFromGenerated() error = %v, want internal error", err)
	}
}

func TestTaskPageFromGeneratedPropagatesUnknownType(t *testing.T) {
	t.Parallel()
	items := []generated.Task{{Type: generated.TaskType("future_task")}}

	_, err := taskPageFromGenerated(generated.PageTask{Items: &items})
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("taskPageFromGenerated() error = %v, want internal error", err)
	}
}
