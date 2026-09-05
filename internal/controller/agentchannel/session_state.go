package agentchannel

import (
	"context"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

type sessionState struct {
	generation         uint64
	fence              uint64
	online             bool
	assignmentsStopped bool
	revoked            bool
	lastReady          time.Time
	readyReported      bool
	capacity           int32
	version            string
	cancel             context.CancelFunc
	dispatchWake       chan struct{}
	sendPermit         chan struct{}
	sendFence          chan struct{}
	sendFenced         bool
	done               <-chan struct{}
	aborts             chan taskAbortCommand
	logCommands        chan logCommand
	offline            chan struct{}
	offlineOnce        sync.Once
	imageCounter       ids.ImageCorrelationCounter
	imageCommands      chan *imageCommand
	activeImage        *imageCommand
}
