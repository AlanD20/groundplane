package controller

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestBuildPlanFailsClosedUntilImplemented(t *testing.T) {
	_, err := BuildPlan("deploy", "prj_test", 1)
	if !errors.Is(err, errs.New(errs.CodeNotImplemented, "")) {
		t.Fatalf("BuildPlan() error = %v, want %q", err, errs.CodeNotImplemented)
	}
}
