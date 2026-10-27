package controllerupgrade

import (
	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"testing"
)

// UP-21: completed protected image cleanup must not permanently block upgrades;
// an active or newly changed guard must still prevent publication.
func TestControllerUpdateAfterImageCleanup(t *testing.T) {
	for _, scenario := range []struct {
		name, record string
		change       bool
		allowed      bool
	}{
		{name: "never removed", allowed: true},
		{name: "completed removal", record: `{"operation":""}`, allowed: true},
		{name: "active removal", record: `{"operation":"operation-active"}`},
		{name: "corrupt guard", record: `not-json`},
		{name: "cleanup races publication", record: `{"operation":""}`, change: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			h := newServiceHarness(t)
			if scenario.record != "" {
				h.storage.entries[imagefence.Key] = keyvalue.KeyValue{
					Key:         imagefence.Key,
					Value:       []byte(scenario.record),
					ModRevision: 1,
				}
			}
			if scenario.change {
				h.storage.beforeCommit = func() {
					h.storage.revision++
					h.storage.entries[imagefence.Key] = keyvalue.KeyValue{
						Key: imagefence.Key, Value: []byte(`{"operation":"new-cleanup"}`), ModRevision: h.storage.revision}
				}
			}
			_, err := h.service.UpdateController(t.Context(), string(h.input.Release), "update-after-image-cleanup")
			if (err == nil) != scenario.allowed {
				t.Fatalf("admitted=%v, want %v: %v", err == nil, scenario.allowed, err)
			}
			if !scenario.allowed && h.storage.transactions != 0 {
				t.Fatal("rejected update published effects")
			}
		})
	}
}
