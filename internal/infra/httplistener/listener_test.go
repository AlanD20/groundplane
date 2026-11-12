package httplistener

import (
	"context"
	"testing"
)

// A tailnet interface may come up after GP at boot. Reserving its exact socket
// must not delay local startup or weaken the exclusive-port check.
func TestTailnetBindingBeforeInterfaceStartup(t *testing.T) {
	ctx := context.Background()
	listener, err := Listen(ctx, "100.127.254.253:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	defer listener.Close()
	duplicate, err := Listen(ctx, address)
	if err == nil {
		duplicate.Close()
		t.Fatal("duplicate listener unexpectedly acquired the reserved port")
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	rebound, err := Listen(ctx, address)
	if err != nil {
		t.Fatal(err)
	}
	rebound.Close()
}
