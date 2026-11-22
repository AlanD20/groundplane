package softwarepreparation

import (
	"context"
	"testing"
)

func TestPreparationRunsConsoleStageAfterDiagnosticLoss(t *testing.T) {
	observer := &preparationObserver{lost: true}
	called := false
	err := observer.Run(t.Context(), "software_console_publish", func(context.Context) error {
		called = true
		return nil
	})
	if err != nil || !called {
		t.Fatalf("Console staging action called = %t, error = %v", called, err)
	}
}
