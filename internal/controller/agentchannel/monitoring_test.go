package agentchannel

import (
	"context"
	"strings"
	"testing"
	"time"

	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A schema-1 mixed-build session must never receive the new message; matching
// version text is insufficient. Maintenance must also see the active read.
func TestMonitoringRequiresExactReadyBuildAndQuiescence(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	r := NewRegistry()
	r.source = buildversion.BuildProvenance{Kind: "release", Commit: strings.Repeat("a", 40)}
	s, err := r.Open(ctx, "agent", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.RecordReady(time.Now(), 0, "0.0.1", buildversion.BuildProvenance{Kind: "release", Commit: strings.Repeat("b", 40)}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CollectMonitoring(ctx, "agent", servingTargets(), nil, false); err == nil {
		t.Fatal("mixed-build Agent accepted monitoring")
	}
	select {
	case <-s.state.monitoringCommands:
		t.Fatal("sent unknown message to mixed-build Agent")
	default:
	}
	s.Close()
	s, err = r.Open(ctx, "agent", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.RecordReady(time.Now(), 0, "0.0.1", r.source); err != nil {
		t.Fatal(err)
	}
	done := make(chan monitoringReply, 1)
	go func() {
		result, err := r.CollectMonitoring(ctx, "agent", servingTargets(), nil, false)
		done <- monitoringReply{result: result, err: err}
	}()
	var command *monitoringCommand
	select {
	case command = <-s.state.monitoringCommands:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	r.mu.Lock()
	active := r.hasActiveAuxiliaryLocked(s.state)
	r.mu.Unlock()
	if !active {
		t.Fatal("monitoring read hidden from maintenance")
	}
	result := &agentpb.MonitoringSamplesResult{
		RequestId: command.request.RequestId,
		Services: []*agentpb.MonitoringServiceSample{{
			ServiceId: command.request.Targets[0].ServiceId, ReleaseId: command.request.Targets[0].ReleaseId, Unavailable: true,
		}},
	}
	s.acceptMonitoringResult(
		ctx,
		&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_MonitoringSamplesResult{MonitoringSamplesResult: result}},
	)
	select {
	case reply := <-done:
		if reply.err != nil || reply.result == nil {
			t.Fatalf("matching exchange failed: %v", reply.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	r.mu.Lock()
	active = r.hasActiveAuxiliaryLocked(s.state)
	r.mu.Unlock()
	if active {
		t.Fatal("finished monitoring retained maintenance hold")
	}
}
