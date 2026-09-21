package controllertask

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	corebackup "github.com/AlanD20/groundplane/internal/core/backup"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type recordingHandler struct {
	calls int
	task  etcd.TaskRecord
}

func (handler *recordingHandler) Execute(_ context.Context, task etcd.TaskRecord) error {
	handler.calls++
	handler.task = task
	return nil
}

// Rationale: production native dispatch must require a concrete rotation
// executor, route only the closed rotation Task shape to it, and preserve the
// existing fallback without an optional or variadic compatibility path.
func TestDispatcherRequiresAndRoutesExactBackupKeyRotationExecutor(t *testing.T) {
	fallback := &recordingHandler{}
	rotations := &recordingHandler{}
	if _, err := NewDispatcher(nil, rotations); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("NewDispatcher(nil fallback) error = %v", err)
	}
	if _, err := NewDispatcher(fallback, nil); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("NewDispatcher(nil rotations) error = %v", err)
	}
	dispatcher, err := NewDispatcher(fallback, rotations)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	task := etcd.TaskRecord{
		ID:          ids.NewAt(ids.KindTask, now, 1),
		OperationID: ids.NewAt(ids.KindOperation, now, 2),
		Owner: testtaskjournal.TaskOwner{
			WorkspaceType: testtaskjournal.TaskWorkspacePlatform,
			ProjectID:     ids.NewAt(ids.KindProject, now, 3),
			EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 4),
		},
		Executor:         testtaskjournal.TaskExecutorController,
		PlanID:           ids.NewAt(ids.KindPlan, now, 5),
		PlanHash:         corebackup.KeyRotationPlanHash(),
		RenderGeneration: 1,
		Type:             testtaskjournal.TaskRotate,
		TimeoutSeconds:   corebackup.KeyRotationTimeoutSeconds,
		Status:           testtaskjournal.TaskStatusRunning,
	}
	task.Target = task.Owner.EnvironmentID
	if err := dispatcher.Execute(context.Background(), task); err != nil {
		t.Fatalf("Execute(rotation) error = %v", err)
	}
	if rotations.calls != 1 || rotations.task.ID != task.ID || fallback.calls != 0 {
		t.Fatalf("rotation/fallback calls = %d/%d", rotations.calls, fallback.calls)
	}

	ordinary := task
	ordinary.Type = testtaskjournal.TaskRemove
	if err := dispatcher.Execute(context.Background(), ordinary); err != nil {
		t.Fatalf("Execute(fallback) error = %v", err)
	}
	if fallback.calls != 1 || rotations.calls != 1 {
		t.Fatalf("fallback/rotation calls = %d/%d", fallback.calls, rotations.calls)
	}

	invalid := task
	invalid.TimeoutSeconds--
	if err := dispatcher.Execute(context.Background(), invalid); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("Execute(invalid rotation) error = %v", err)
	}
}
