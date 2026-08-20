package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestSchedulerTickFailsClosedUntilImplemented(t *testing.T) {
	scheduler := NewScheduler(&Server{}, 1)
	err := scheduler.tick(context.Background())
	if !errors.Is(err, errs.New(errs.KindNotImplemented, "")) {
		t.Fatalf("tick() error = %v, want %q", err, errs.CodeNotImplemented)
	}
}

func TestSchedulerTickHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := NewScheduler(&Server{}, 1).tick(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("tick() error = %v, want context.Canceled", err)
	}
}
