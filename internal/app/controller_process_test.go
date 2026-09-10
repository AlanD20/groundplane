package app

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: private recovery dispatch is not an arbitrary native command
// surface, and invalid arguments must not initialize Controller or host state.
func TestControllerProcessRejectsOpenEndedArguments(t *testing.T) {
	for _, args := range [][]string{{"--exec", "sh"}, {"--upgrade-guard", "extra"}, {"--upgrade-run"}, {"--upgrade-run", "task", "extra"}} {
		if err := RunControllerProcess(context.Background(), args); !errors.Is(
			err,
			errs.New(errs.KindValidationFailed, ""),
		) {
			t.Fatalf("args=%q error=%v", args, err)
		}
	}
}
