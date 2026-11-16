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
			response, err := h.service.UpdateController(
				t.Context(),
				string(h.input.Release),
				nil,
				"update-after-image-cleanup",
			)
			if err != nil {
				t.Fatal(err)
			}
			h.prepare(t, response.Body)
			_, found, err := h.service.records.Current(t.Context(), 0)
			if err != nil || found != scenario.allowed {
				t.Fatalf("captured=%v, want %v: %v", found, scenario.allowed, err)
			}

		})
	}
}
