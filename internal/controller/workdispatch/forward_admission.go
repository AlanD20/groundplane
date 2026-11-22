package workdispatch

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ForwardGate drains new scheduling and Start grants without blocking result
// replay or reconciliation. One runtime pause owns the gate until recovery has
// verified the same Controller executor identity.
type ForwardGate struct {
	mu      sync.Mutex
	paused  bool
	active  uint64
	drained chan struct{}
}

type ForwardPause struct {
	gate    *ForwardGate
	drained <-chan struct{}
	once    sync.Once
	result  error
}

func NewForwardGate() *ForwardGate { return &ForwardGate{} }

// Begin admits one complete scheduling, delivery or fresh-Start operation. A
// false result means storage recovery has paused new forward work.
func (gate *ForwardGate) Begin(ctx context.Context) (func(), bool) {
	if gate == nil || ctx == nil || ctx.Err() != nil {
		return nil, false
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.paused || ctx.Err() != nil {
		return nil, false
	}
	gate.active++
	var once sync.Once
	return func() {
		once.Do(func() {
			gate.mu.Lock()
			defer gate.mu.Unlock()
			if gate.active == 0 {
				return
			}
			gate.active--
			if gate.paused && gate.active == 0 {
				close(gate.drained)
			}
		})
	}, true
}

// Pause closes admission immediately. Wait is separate so the runtime can
// cancel admitted Controller workers before waiting for them to drain.
func (gate *ForwardGate) Pause() (*ForwardPause, error) {
	if gate == nil {
		return nil, errs.New(errs.KindInternal, "forward execution admission is unavailable")
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.paused {
		return nil, errs.New(errs.KindStateConflict, "forward execution admission is already paused")
	}
	gate.paused = true
	gate.drained = make(chan struct{})
	if gate.active == 0 {
		close(gate.drained)
	}
	return &ForwardPause{gate: gate, drained: gate.drained}, nil
}

func (pause *ForwardPause) Wait(ctx context.Context) error {
	if pause == nil || pause.gate == nil || ctx == nil {
		return errs.New(errs.KindInternal, "forward execution pause is invalid")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-pause.drained:
		return nil
	}
}

func (pause *ForwardPause) Resume() error {
	if pause == nil || pause.gate == nil {
		return errs.New(errs.KindInternal, "forward execution pause is invalid")
	}
	pause.once.Do(func() {
		gate := pause.gate
		gate.mu.Lock()
		defer gate.mu.Unlock()
		if !gate.paused || gate.drained != pause.drained {
			pause.result = errs.New(errs.KindStateConflict, "forward execution pause ownership changed")
			return
		}
		if gate.active != 0 {
			pause.result = errs.New(errs.KindStateConflict, "forward execution is not drained")
			return
		}
		gate.paused = false
		gate.drained = nil
	})
	return pause.result
}
