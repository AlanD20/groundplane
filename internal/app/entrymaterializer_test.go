package app

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestRunEntryMaterializerRejectsMalformedFrameBeforeFilesystemAccess(t *testing.T) {
	// Rationale: helper mode must use the authenticated frame decoder with
	// fixed production limits rather than treating stdin as trusted file bytes.
	err := RunEntryMaterializer(
		context.Background(),
		io.NopCloser(strings.NewReader("not-a-materialization-frame")),
	)
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("RunEntryMaterializer() error = %v, want internal protocol failure", err)
	}
}

func TestRunEntryMaterializerRequiresOwnedInput(t *testing.T) {
	// Rationale: a nil context or source would bypass the helper's explicit
	// stream ownership and cancellation contract.
	var nilOperationContext context.Context
	if err := RunEntryMaterializer(
		nilOperationContext,
		io.NopCloser(strings.NewReader("")),
	); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("RunEntryMaterializer(nil context) error = %v", err)
	}
	if err := RunEntryMaterializer(context.Background(), nil); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("RunEntryMaterializer(nil source) error = %v", err)
	}
}
