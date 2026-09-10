package controllerupgrade

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: executing a native Task proves neither HTTP nor channel binding.
// Candidate qualification must await both listeners and a fresh storage read.
func TestReadinessRequiresBothListenersAndHealthyStorage(t *testing.T) {
	for _, scenario := range []string{"http", "channel", "storage", "ready"} {
		t.Run(scenario, func(t *testing.T) {
			storage := &readinessStorage{}
			ready, err := NewReadiness(storage)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario != "http" {
				ready.MarkHTTPReady()
			}
			if scenario != "channel" {
				ready.MarkChannelReady()
			}
			if scenario == "http" || scenario == "channel" {
				cancel()
			}
			if scenario == "storage" {
				storage.err = errs.New(errs.KindStorageUnavailable, "not ready")
			}
			err = ready.Ready(ctx)
			if scenario == "ready" && err != nil || scenario != "ready" && err == nil {
				t.Fatalf("readiness = %v", err)
			}
			if scenario == "storage" && !errors.Is(err, storage.err) {
				t.Fatalf("storage readiness = %v", err)
			}
			ready.MarkHTTPReady()
			ready.MarkChannelReady()
		})
	}
}

type readinessStorage struct{ err error }

func (storage *readinessStorage) Health(context.Context) error { return storage.err }
