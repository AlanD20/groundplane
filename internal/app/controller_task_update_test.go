package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: platform updates have one recovery-aware execution path. Falling
// through the ordinary handler would bypass startup holds and timeout recovery.
func TestControllerTaskHandlerCannotBypassAgentUpdateRecovery(t *testing.T) {
	handler, err := newControllerTaskHandler(
		&fakeControllerTaskLocalAgents{}, testBackingZoneCascade(t), &fakeControllerTaskRunners{},
	)
	if err != nil {
		t.Fatal(err)
	}
	task, err := newAgentUpdateTask(
		time.Now().
			UTC(),
		testAgentUpdateID,
		testAgentUpdatePreviousImage,
		testAgentUpdateDesiredImage,
		7,
		"update-test-key",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Execute(context.Background(), task); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("fallback update = %v", err)
	}
}
