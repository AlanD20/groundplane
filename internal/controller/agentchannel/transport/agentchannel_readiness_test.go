package transport

import (
	"context"
	"net"
	"testing"
	"time"
)

// Rationale: opening a socket alone is insufficient; qualification waits until
// the actual gRPC server has entered Accept and can serve the Agent channel.
func TestAgentChannelSignalsReadinessWhenServing(t *testing.T) {
	runtime := New(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	ready := make(chan struct{})
	runtime.OnReady = func() { close(ready) }
	socket := shortUnixSocketPath(t)
	runtime.listen = func(context.Context) (net.Listener, error) { return net.Listen("unix", socket) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("channel failed before readiness: %v", err)
	case <-time.After(time.Second):
		t.Fatal("channel did not signal readiness")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
