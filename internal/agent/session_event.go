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
	sessionLog
	sessionNative
	sessionNativeResult
)

type sessionEvent struct {
	kind          sessionEventKind
	observation   *agentpb.ServiceObservationResult
	backingAccess *agentpb.BackingAccessResult
	dnsCheck      *agentpb.DNSConnectionResult
	edgeCheck     *agentpb.EdgeConnectionResult
	image         imageOutput
	received      receiveResult
	log           *agentpb.AgentMessage
	native        *agentpb.AgentMessage
	nativeResult  executionOutput
}

// One stream writer multiplexes native operations and read-only services.
// Operation-specific flow control bounds bulk delivery; the retired worker pool
// contributes no work, progress, acknowledgement or independent bulk channel.
func nextSessionEvent(
	ctx context.Context,
	observations *observationSession,
	images *imageSession,
	backingAccess *backingAccessSession,
	dnsChecks *dnsCheckSession,
	edgeChecks *edgeCheckSession,
	received <-chan receiveResult,
	logs <-chan *agentpb.AgentMessage,
	ticks <-chan time.Time,
	native <-chan *agentpb.AgentMessage,
	nativeResults <-chan executionOutput,
) sessionEvent {
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
	case message := <-native:
		return sessionEvent{kind: sessionNative, native: message}
	case result := <-nativeResults:
		return sessionEvent{kind: sessionNativeResult, nativeResult: result}
	case message := <-logs:
		return sessionEvent{kind: sessionLog, log: message}
	}
}
