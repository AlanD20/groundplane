package controllerrelease

import (
	"context"
	"errors"
	"testing"
)

// Rationale: qualification identifies the running inode, not the installation
// path which a watchdog can replace before the predecessor process exits.
func TestExecutingDigestIsStableAndHonorsCancellation(t *testing.T) {
	first, err := ExecutingDigest(context.Background())
	if err != nil || !first.Valid() {
		t.Fatalf("executing digest = %q, %v", first, err)
	}
	second, err := ExecutingDigest(context.Background())
	if err != nil || second != first {
		t.Fatalf("second executing digest = %q, %v", second, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ExecutingDigest(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled digest = %v", err)
	}
}
