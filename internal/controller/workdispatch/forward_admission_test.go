package workdispatch

import (
	"context"
	"errors"
	"testing"
)

func TestForwardPauseDrainsAcceptedWorkBeforeReopening(t *testing.T) {
	gate := NewForwardGate()
	release, allowed := gate.Begin(t.Context())
	if !allowed || release == nil {
		t.Fatal("initial forward work was not admitted")
	}
	pause, err := gate.Pause()
	if err != nil {
		t.Fatal(err)
	}
	if _, allowed := gate.Begin(t.Context()); allowed {
		t.Fatal("forward work was admitted while storage recovery was paused")
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := pause.Wait(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("pause did not retain admitted work: %v", err)
	}
	release()
	if err := pause.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := pause.Resume(); err != nil {
		t.Fatal(err)
	}
	resumed, allowed := gate.Begin(t.Context())
	if !allowed || resumed == nil {
		t.Fatal("forward work did not resume after the admitted operation drained")
	}
	resumed()
}
