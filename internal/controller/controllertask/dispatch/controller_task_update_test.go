package dispatch

import (
	"context"
	"errors"
	"testing"

	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: platform updates have one recovery-aware execution path. Falling
// through the ordinary handler would bypass startup holds and timeout recovery.
func TestControllerTaskHandlerCannotBypassAgentUpdateRecovery(t *testing.T) {
	handler, err := NewResourceHandler(
		&fakeControllerTaskLocalAgents{}, testBackingZoneCascade(t), &fakeControllerTaskRunners{},
	)
	if err != nil {
		t.Fatal(err)
	}
	task := testAgentEnrollmentTask()
	task.Type = testtaskjournal.TaskUpdate
	if err := handler.Execute(context.Background(), task); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("fallback update = %v", err)
	}
}
