package app

import (
	"bytes"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// TASK-200: acknowledged Component metadata must publish with its completion
// fragment, or not at all if a concurrent metadata write wins. Authored intent
// is separate. This is an atomic-CAS model; live execution qualifies its caller.
func TestNativeComponentMetadataPublicationIsAtomic(t *testing.T) {
	for _, raced := range []bool{false, true} {
		t.Run(map[bool]string{false: "publish", true: "concurrent metadata"}[raced], func(t *testing.T) {
			f := newNativeComponentAdmissionFixture(t)
			_, capture, _, err := f.inputs.Load(t.Context(), f.view)
			if err != nil {
				t.Fatal(err)
			}
			applied := components.CloneRecord(capture.Current)
			applied.Desired.Enabled = true
			applied.Desired.Config.Caddy = &core.CaddyComponentConfig{ZoneIDs: []string{ids.New(ids.KindNetwork)}}
			applied.Runtime.GeneratedServices = []string{ids.New(ids.KindService)}
			applied.Runtime.PinnedIPv4, applied.Runtime.Healthy = "10.40.0.2", true
			metadata, err := componentplanning.PrepareComponentMetadataPublication(t.Context(), f.store,
				f.view.Task.Task.Scope.ProjectID, keyvalue.Versioned[components.Record]{Record: capture.Current,
					Revision: capture.CurrentRevision, ReadRevision: f.view.ReadRevision}, applied)
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearMutationValues(metadata.Mutations)
			key := components.RecordKey(applied.Desired.ID)
			headKey := blueprints.EnvironmentBlueprintHeadKey(f.environmentID)
			head := bytes.Clone(f.store.valueAt(headKey, f.store.revision).Value)
			if raced {
				if _, err := f.store.Put(t.Context(), key, f.store.valueAt(key, f.store.revision).Value); err != nil {
					t.Fatal(err)
				}
			}
			ackKey := "/test/component-completion-receipt"
			result, err := f.queue.Commit(t.Context(), metadata, workqueue.Change{Mutations: []keyvalue.Mutation{
				{Type: keyvalue.MutationPut, Key: ackKey, Value: []byte("acknowledged")},
			}})
			if err != nil || result.Succeeded == raced {
				t.Fatalf("Component metadata publication: %v/%v", result, err)
			}
			record, err := components.DecodeRecord(f.store.valueAt(key, f.store.revision).Value)
			if err != nil {
				t.Fatal(err)
			}
			if raced {
				if record.Desired.Enabled || f.store.valueAt(ackKey, f.store.revision) != nil {
					t.Fatal("stale publication changed metadata or recorded partial completion")
				}
			} else if !record.Desired.Enabled || !record.Runtime.Healthy ||
				record.Runtime.PinnedIPv4 != "10.40.0.2" || len(record.Runtime.GeneratedServices) != 1 ||
				record.Runtime.GeneratedServices[0] != applied.Runtime.GeneratedServices[0] ||
				f.store.valueAt(ackKey, f.store.revision) == nil {
				t.Fatal("completion left disabled or empty Component metadata")
			}
			if !bytes.Equal(head, f.store.valueAt(headKey, f.store.revision).Value) {
				t.Fatal("applied metadata rewrote authored Blueprint intent")
			}
		})
	}
}
