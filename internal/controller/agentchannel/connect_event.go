package agentchannel

import "github.com/AlanD20/groundplane/proto/agentpb"

type agentReceiveResult struct {
	message *agentpb.AgentMessage
	err     error
}

type connectEventKind uint8

const (
	connectEnded connectEventKind = iota
	connectObservationExpired
	connectObservationCommand
	connectBackingAccessExpired
	connectBackingAccessCommand
	connectDNSCheckExpired
	connectDNSCheckCommand
	connectEdgeCheckExpired
	connectEdgeCheckCommand
	connectImageCommand
	connectTaskAbort
	connectLogCommand
	connectDispatchWake
	connectReceived
	connectPayloadFrame
	connectPayloadFailed
)

type connectEvent struct {
	kind          connectEventKind
	observation   *observationCommand
	backingAccess *backingAccessCommand
	dnsCheck      *dnsCheckCommand
	edgeCheck     *edgeCheckCommand
	image         *imageCommand
	abort         taskAbortCommand
	log           logCommand
	received      agentReceiveResult
	frame         *assignmentPayloadFrame
	err           error
}

const maximumControlBurst = 4

// Prefer control traffic, but permit at most four control events before an
// already queued bulk frame. Each bulk producer has at most one queued frame;
// a continuous stream cannot starve another assignment or control delivery.
func nextConnectEvent(
	session *Session,
	observations *observationExchange,
	backingAccess *backingAccessExchange,
	dnsChecks *dnsCheckExchange,
	edgeChecks *edgeCheckExchange,
	received <-chan agentReceiveResult,
	payloads *assignmentPayloadDelivery,
	controlBurst int,
) connectEvent {
	select {
	case <-session.Done():
		return connectEvent{kind: connectEnded}
	default:
	}
	if controlBurst >= maximumControlBurst {
		select {
		case frame := <-payloads.frames:
			return connectEvent{kind: connectPayloadFrame, frame: frame}
		default:
		}
	}
	select {
	case <-session.Done():
		return connectEvent{kind: connectEnded}
	case err := <-payloads.failures:
		return connectEvent{kind: connectPayloadFailed, err: err}
	case <-observations.done():
		return connectEvent{kind: connectObservationExpired}
	case command := <-session.state.observationCommands:
		return connectEvent{kind: connectObservationCommand, observation: command}
	case <-backingAccess.done():
		return connectEvent{kind: connectBackingAccessExpired}
	case command := <-session.state.backingAccessCommands:
		return connectEvent{kind: connectBackingAccessCommand, backingAccess: command}
	case <-dnsChecks.done():
		return connectEvent{kind: connectDNSCheckExpired}
	case command := <-session.state.dnsCheckCommands:
		return connectEvent{kind: connectDNSCheckCommand, dnsCheck: command}
	case <-edgeChecks.done():
		return connectEvent{kind: connectEdgeCheckExpired}
	case command := <-session.state.edgeCheckCommands:
		return connectEvent{kind: connectEdgeCheckCommand, edgeCheck: command}
	case command := <-session.state.imageCommands:
		return connectEvent{kind: connectImageCommand, image: command}
	case abort := <-session.taskAborts():
		return connectEvent{kind: connectTaskAbort, abort: abort}
	case command := <-session.logMessages():
		return connectEvent{kind: connectLogCommand, log: command}
	case <-session.taskDispatchWake():
		return connectEvent{kind: connectDispatchWake}
	case result := <-received:
		return connectEvent{kind: connectReceived, received: result}
	default:
	}
	select {
	case <-session.Done():
		return connectEvent{kind: connectEnded}
	case err := <-payloads.failures:
		return connectEvent{kind: connectPayloadFailed, err: err}
	case <-observations.done():
		return connectEvent{kind: connectObservationExpired}
	case command := <-session.state.observationCommands:
		return connectEvent{kind: connectObservationCommand, observation: command}
	case <-backingAccess.done():
		return connectEvent{kind: connectBackingAccessExpired}
	case command := <-session.state.backingAccessCommands:
		return connectEvent{kind: connectBackingAccessCommand, backingAccess: command}
	case <-dnsChecks.done():
		return connectEvent{kind: connectDNSCheckExpired}
	case command := <-session.state.dnsCheckCommands:
		return connectEvent{kind: connectDNSCheckCommand, dnsCheck: command}
	case <-edgeChecks.done():
		return connectEvent{kind: connectEdgeCheckExpired}
	case command := <-session.state.edgeCheckCommands:
		return connectEvent{kind: connectEdgeCheckCommand, edgeCheck: command}
	case command := <-session.state.imageCommands:
		return connectEvent{kind: connectImageCommand, image: command}
	case abort := <-session.taskAborts():
		return connectEvent{kind: connectTaskAbort, abort: abort}
	case command := <-session.logMessages():
		return connectEvent{kind: connectLogCommand, log: command}
	case <-session.taskDispatchWake():
		return connectEvent{kind: connectDispatchWake}
	case result := <-received:
		return connectEvent{kind: connectReceived, received: result}
	case frame := <-payloads.frames:
		return connectEvent{kind: connectPayloadFrame, frame: frame}
	}
}
