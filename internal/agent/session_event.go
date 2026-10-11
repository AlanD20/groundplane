package agent

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

type sessionEventKind uint8

const (
	sessionEnded sessionEventKind = iota
	sessionObservation
	sessionBackingAccess
	sessionDNSCheck
	sessionEdgeCheck
	sessionImage
	sessionReadiness
	sessionReceived
	sessionWorker
	sessionLog
	sessionBulk
)

type sessionEvent struct {
	kind          sessionEventKind
	observation   *agentpb.ServiceObservationResult
	backingAccess *agentpb.BackingAccessResult
	dnsCheck      *agentpb.DNSConnectionResult
	edgeCheck     *agentpb.EdgeConnectionResult
	image         imageOutput
	received      receiveResult
	worker        WorkerOutput
	log           *agentpb.AgentMessage
	bulk          *backupBulkFrame
}

// Prefer ready control traffic, but force a queued bulk frame after at most
// four control events. After each bulk frame, ready control wins again. Only
// the session writer consumes these channels or writes the underlying stream.
func nextSessionEvent(
	ctx context.Context,
	pool *WorkerPool,
	observations *observationSession,
	images *imageSession,
	backingAccess *backingAccessSession,
	dnsChecks *dnsCheckSession,
	edgeChecks *edgeCheckSession,
	received <-chan receiveResult,
	logs <-chan *agentpb.AgentMessage,
	ticks <-chan time.Time,
	controlBurst int,
) sessionEvent {
	select {
	case <-ctx.Done():
		return sessionEvent{kind: sessionEnded}
	default:
	}
	if controlBurst >= maximumControlBurst {
		select {
		case frame := <-pool.bulkFrames:
			return sessionEvent{kind: sessionBulk, bulk: frame}
		default:
		}
	}
	select {
	case <-ctx.Done():
		return sessionEvent{kind: sessionEnded}
	case result := <-observations.outputs:
		return sessionEvent{kind: sessionObservation, observation: result}
	case result := <-backingAccess.outputs:
		return sessionEvent{kind: sessionBackingAccess, backingAccess: result}
	case result := <-dnsChecks.outputs:
		return sessionEvent{kind: sessionDNSCheck, dnsCheck: result}
	case result := <-edgeChecks.outputs:
		return sessionEvent{kind: sessionEdgeCheck, edgeCheck: result}
	case result := <-images.outputs:
		return sessionEvent{kind: sessionImage, image: result}
	case <-ticks:
		return sessionEvent{kind: sessionReadiness}
	case result := <-received:
		return sessionEvent{kind: sessionReceived, received: result}
	case output := <-pool.Outputs():
		return sessionEvent{kind: sessionWorker, worker: output}
	case message := <-logs:
		return sessionEvent{kind: sessionLog, log: message}
	default:
	}
	select {
	case <-ctx.Done():
		return sessionEvent{kind: sessionEnded}
	case result := <-observations.outputs:
		return sessionEvent{kind: sessionObservation, observation: result}
	case result := <-backingAccess.outputs:
		return sessionEvent{kind: sessionBackingAccess, backingAccess: result}
	case result := <-dnsChecks.outputs:
		return sessionEvent{kind: sessionDNSCheck, dnsCheck: result}
	case result := <-edgeChecks.outputs:
		return sessionEvent{kind: sessionEdgeCheck, edgeCheck: result}
	case result := <-images.outputs:
		return sessionEvent{kind: sessionImage, image: result}
	case <-ticks:
		return sessionEvent{kind: sessionReadiness}
	case result := <-received:
		return sessionEvent{kind: sessionReceived, received: result}
	case output := <-pool.Outputs():
		return sessionEvent{kind: sessionWorker, worker: output}
	case message := <-logs:
		return sessionEvent{kind: sessionLog, log: message}
	case frame := <-pool.bulkFrames:
		return sessionEvent{kind: sessionBulk, bulk: frame}
	}
}
