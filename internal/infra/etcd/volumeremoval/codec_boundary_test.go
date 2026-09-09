package volumeremoval

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
)

// Rationale: the publication owner and runtime owner must share one canonical
// representation. Moving its package must not rewrite accepted record bytes.
func TestVolumeRemovalCanonicalRecordEncoding(t *testing.T) {
	_, _, runtime, _, _ := environmentVolumeRemovalRuntimeFixture(t)
	attempt := removalrecord.Attempt{
		OperationID: runtime.OperationID, OriginTaskID: runtime.OriginTaskID,
		TaskID: runtime.CurrentTaskID, Ordinal: 1, CreatedAt: runtime.CreatedAt,
	}
	progress := removalrecord.Progress{
		OperationID: runtime.OperationID, NextRequestOrdinal: 1, UpdatedAt: runtime.CreatedAt,
	}
	runtimeValue, err := removalrecord.EncodeRuntime(runtime)
	if err != nil {
		t.Fatal(err)
	}
	attemptValue, err := removalrecord.EncodeAttempt(attempt)
	if err != nil {
		t.Fatal(err)
	}
	progressValue, err := removalrecord.EncodeProgress(progress)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string][]byte{"runtime": runtimeValue, "attempt": attemptValue, "progress": progressValue} {
		want := map[string]string{
			"runtime":  "9478508a4c4c2a997f79deb3c7d7e1851e42dc89aeda2c0d2875c876143bdc6e",
			"attempt":  "8f2be9a2ae90eb773095a158caac557808dd267f2715cad9d15b2f4924ce0f31",
			"progress": "5f077cabe49d3fc5808968890bc378dc806cd4a842c2e27c3bfab3e966483cee",
		}[name]
		if got := fmt.Sprintf("%x", sha256.Sum256(value)); got != want {
			t.Fatalf("%s encoding changed: got %s want %s", name, got, want)
		}
		clear(value)
	}
	if got := removalrecord.RuntimeKey(runtime.OperationID); got != "/v1/runtime/environment-volume-removals/"+
		etcd.EncodeCapabilityKeySegment(runtime.OperationID)+"/runtime" {
		t.Fatal("record extraction changed the existing runtime key")
	}
}

// Rationale: the neutral replay record must preserve the marker boundary's
// accepted identity grammar without gaining marker persistence authority.
func TestVolumeRemovalRecordPreservesReplayLocatorValidation(t *testing.T) {
	_, _, runtime, _, _ := environmentVolumeRemovalRuntimeFixture(t)
	for name, change := range map[string]func(*removalrecord.ReplayLocator){
		"unchanged":        func(*removalrecord.ReplayLocator) {},
		"delete":           func(locator *removalrecord.ReplayLocator) { locator.Method = "DELETE" },
		"read method":      func(locator *removalrecord.ReplayLocator) { locator.Method = "GET" },
		"lowercase method": func(locator *removalrecord.ReplayLocator) { locator.Method = "delete" },
		"empty route":      func(locator *removalrecord.ReplayLocator) { locator.Route = "" },
		"relative route":   func(locator *removalrecord.ReplayLocator) { locator.Route = "volumes" },
		"invalid UTF-8":    func(locator *removalrecord.ReplayLocator) { locator.Route = "/\xff" },
		"short key":        func(locator *removalrecord.ReplayLocator) { locator.Key = "short" },
		"long key":         func(locator *removalrecord.ReplayLocator) { locator.Key = strings.Repeat("a", 129) },
		"overlong marker":  func(locator *removalrecord.ReplayLocator) { locator.Route = "/" + strings.Repeat("a", 1800) },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := runtime
			change(&candidate.RootLocator)
			_, markerErr := etcd.CapabilityIdempotencyMarkerKey(volumeRemovalRootLocator(candidate))
			value, recordErr := removalrecord.EncodeRuntime(candidate)
			defer clear(value)
			if (markerErr == nil) != (recordErr == nil) {
				t.Fatalf("record/marker locator validation diverged: marker=%v record=%v", markerErr, recordErr)
			}
		})
	}
}
