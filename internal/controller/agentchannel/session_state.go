package agentchannel

import (
	"context"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

type sessionState struct {
	generation          uint64
	fence               uint64
	online              bool
	assignmentsStopped  bool
	revoked             bool
	lastReady           time.Time
	readyReported       bool
	capacity            int32
	version             string
	cancel              context.CancelFunc
	dispatchWake        chan struct{}
	sendPermit          chan struct{}
	sendFence           chan struct{}
	sendFenced          bool
	done                <-chan struct{}
	aborts              chan taskAbortCommand
	logCommands         chan logCommand
	offline             chan struct{}
	offlineOnce         sync.Once
	imageCounter        ids.ImageCorrelationCounter
	imageCommands       chan *imageCommand
	activeImage         *imageCommand
	observationCommands chan *observationCommand
	activeObservation   *observationCommand
}

func newSessionState(
	ctx context.Context,
	cancel context.CancelFunc,
	generation, fence uint64,
	stopped bool,
) *sessionState {
	sendPermit := make(chan struct{}, 1)
	sendPermit <- struct{}{}
	state := &sessionState{
		generation: generation, fence: fence, online: true, assignmentsStopped: stopped,
		cancel: cancel, dispatchWake: make(chan struct{}, 1), sendPermit: sendPermit,
		sendFence: make(chan struct{}), done: ctx.Done(), aborts: make(chan taskAbortCommand),
		logCommands: make(chan logCommand), imageCommands: make(chan *imageCommand),
		imageCounter: ids.NewImageCorrelationCounter(), offline: make(chan struct{}),
		observationCommands: make(chan *observationCommand),
	}
	if stopped {
		state.fenceAssignmentSendsLocked()
	}
	return state
}
