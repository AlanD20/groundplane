package app

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/app/servicereadplatform"
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/controller/monitoring"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingservices"
)

// A corrupt derived history file must remain preserved for diagnosis while
// the real host sampler, read API and ordinary parent lifetime keep running.
func TestMonitoringHistoryFailureDoesNotPreventLiveHostReads(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("native Host usage reads require Linux")
	}
	store := newMemoryHierarchyStore()
	store.revision = 1
	hierarchy, err := etcd.NewHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	services, err := etcd.NewServiceRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	zones, err := etcd.NewZoneRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := etcd.NewTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	releases, err := etcd.NewReleaseLedger(store, tasks)
	if err != nil {
		t.Fatal(err)
	}
	agents, err := agentregistration.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	backing, err := backingservices.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	consumers, err := etcd.NewAttachRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "broken.sqlite")
	evidence := []byte("preserve this invalid derived history file")
	if err := os.WriteFile(path, evidence, 0600); err != nil {
		t.Fatal(err)
	}
	reads, err := servicereadplatform.New(t.Context(), servicereadplatform.Dependencies{
		Hierarchy: hierarchy, Services: services, Zones: zones, Releases: releases, Agents: agents,
		Channel: agentchannel.NewRegistry(), Backing: backing, Consumers: consumers,
		Logger: slog.New(slog.DiscardHandler), HistoryPath: path,
	})
	if err != nil {
		t.Fatalf("corrupt derived history prevented composition: %v", err)
	}
	defer func() { _ = reads.Close() }()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { reads.Run(ctx); close(done) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("monitoring worker did not join")
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		response, err := reads.Monitoring.Show(t.Context(), monitoring.Query{Kind: "host", ID: monitoring.LocalHost})
		if err != nil {
			t.Fatal(err)
		}
		if response.Host != nil && response.Host.Live.Status == "fresh" {
			if response.HistoryStatus != "unavailable" || response.Host.Live.Values.MemoryTotalBytes == nil ||
				*response.Host.Live.Values.MemoryTotalBytes.Average <= 0 {
				t.Fatalf("live/history isolation failed: %#v", response)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("real live Host sampling stopped with corrupt history")
		}
		select {
		case <-done:
			t.Fatal("optional monitoring worker exited before cancellation")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if ctx.Err() != nil {
		t.Fatal("history failure canceled parent lifetime")
	}
	preserved, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(preserved, evidence) {
		t.Fatal("corrupt derived history was overwritten or deleted")
	}
}
