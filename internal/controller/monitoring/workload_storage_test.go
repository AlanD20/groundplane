package monitoring

import (
	"strings"
	"testing"
	"time"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	localagentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
)

func TestWorkloadStorageKeepsVolumeSampleAndDropsReplacedContainerLayer(t *testing.T) {
	clock := &workloadSteppingClock{current: time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)}
	channel := &workloadTestChannel{observedNow: clock.Now}
	collector := newWorkloadTestCollector(
		t,
		&workloadTestAgents{records: []etcdstore.Versioned[localagentrecord.LocalAgentRecord]{workloadAgent(1)}},
		&workloadTestSources{recheck: true, expected: 1, storage: true},
		channel,
		nil,
		clock.Now,
	)
	first, err := collector.collect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	stored := first.snapshot.Services[0]
	if stored.StorageObservedAt.IsZero() || stored.Point.WritableLayerBytes == nil ||
		stored.Point.PersistentVolumeUsedBytes == nil {
		t.Fatalf("initial storage read is incomplete: %+v", stored.Point)
	}

	clock.Advance(5 * time.Second)
	unchanged, err := collector.collect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	preservePendingStorage(first, &unchanged)
	if unchanged.snapshot.Services[0].Point.WritableLayerBytes == nil ||
		unchanged.measurements[0].Point.WritableLayerBytes == nil {
		t.Fatal("same container lost its writable-layer reading between storage samples")
	}

	channel.containerID = strings.Repeat("b", 64)
	clock.Advance(5 * time.Second)
	replaced, err := collector.collect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	preservePendingStorage(unchanged, &replaced)
	live := replaced.snapshot.Services[0]
	history := replaced.measurements[0]
	if live.Containers[0].ID != channel.containerID || live.SourceFingerprint != stored.SourceFingerprint {
		t.Fatal("expected a different container under the same serving source")
	}
	if live.Point.WritableLayerBytes != nil || live.Summaries.WritableLayerBytes.Average != nil ||
		history.Point.WritableLayerBytes != nil || history.Summaries.WritableLayerBytes.Average != nil {
		t.Fatalf(
			"replaced container inherited the prior writable layer: live=%+v history=%+v",
			live.Point,
			history.Point,
		)
	}
	if live.Point.PersistentVolumeUsedBytes == nil || history.Point.PersistentVolumeUsedBytes == nil ||
		*live.Point.PersistentVolumeUsedBytes != *stored.Point.PersistentVolumeUsedBytes ||
		*history.Point.PersistentVolumeUsedBytes != *stored.Point.PersistentVolumeUsedBytes ||
		!live.StorageObservedAt.Equal(stored.StorageObservedAt) {
		t.Fatal("persistent Volume lost its previous sampled capacity or timestamp")
	}
}
