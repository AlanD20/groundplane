package etcd

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/remoteimages"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestRemoteReleaseAuthorityFencesForwardEffects(t *testing.T) {
	for _, change := range []string{"agent replacement", "readiness changed", "image replaced", "attachment added"} {
		t.Run(change, func(t *testing.T) {
			store := &releaseRenderInputTestStore{memoryTaskStore: newMemoryTaskStore()}
			ledger := &ReleaseLedger{store: store}
			put := func(key string, value []byte) int64 {
				t.Helper()
				revision, err := store.Put(t.Context(), key, value)
				if err != nil || revision <= 0 {
					t.Fatalf("write fixture %q: revision=%d, err=%v", key, revision, err)
				}
				return revision
			}
			executor := workqueue.ExecutorRecord{
				Identity: workexecution.Identity{Executor: workexecution.Agent, HostID: ids.New(ids.KindHost),
					AgentID: ids.New(ids.KindAgent), Generation: 1},
				Ready: true, Capacity: 1, MaxConcurrentTasks: 1, MaxParallelOperationsPerTask: 1,
				ProcessGeneration: [16]byte{1}, Operations: []workexecution.Operation{workexecution.Release},
			}
			executorKey := workqueue.ExecutorKey(executor.Identity)
			writeExecutor := func() {
				t.Helper()
				raw, err := workqueue.EncodeExecutor(executor)
				if err != nil {
					t.Fatal(err)
				}
				put(executorKey, raw)
			}
			writeExecutor()
			executorRevision := store.currentRevision()
			imageKey := remoteimages.WitnessKey(
				executor.Identity.HostID,
				"registry.example/app@sha256:"+strings.Repeat("a", 64),
			)
			imageRevision := put(imageKey, []byte("image receipt"))
			readyKey, attachPrefix := "/remote/environment/ready", "/remote/environment/attachments/"
			captureRevision := put(readyKey, []byte("ready receipt"))
			source, err := CaptureDirectReleaseRemoteSource(executor.Identity, []keyvalue.Condition{
				{Key: executorKey, ModRevision: executorRevision}, {Key: imageKey, ModRevision: imageRevision},
				{Key: readyKey, ModRevision: captureRevision}, {Key: attachPrefix, Prefix: true},
			}, captureRevision)
			if err != nil {
				t.Fatal(err)
			}

			// An availability update may rewrite the executor but cannot invalidate
			// the selected process. Forward admission must use its latest revision.
			executor.Capacity, executor.MaxConcurrentTasks = 2, 2
			writeExecutor()
			guards, err := ledger.DirectReleaseRemoteConditions(t.Context(), source, store.currentRevision())
			if err != nil {
				t.Fatalf("same executor after capacity change: %v", err)
			}
			switch change {
			case "agent replacement":
				executor.Identity.Generation++
				executor.ProcessGeneration[0]++
				writeExecutor()
			case "readiness changed":
				put(readyKey, []byte("new readiness receipt"))
			case "image replaced":
				put(imageKey, []byte("new image receipt"))
			case "attachment added":
				put(attachPrefix+"attach", []byte("new attachment"))
			}

			effect := keyvalue.Mutation{
				Type:  keyvalue.MutationPut,
				Key:   "/test/forward-effect",
				Value: []byte("granted"),
			}
			result, err := store.Transact(t.Context(), guards, []keyvalue.Mutation{effect})
			if err != nil || result.Succeeded {
				t.Fatalf("stale snapshot allowed forward effect: succeeded=%t, err=%v", result.Succeeded, err)
			}
			if _, err := ledger.DirectReleaseRemoteConditions(t.Context(), source, store.currentRevision()); err == nil {
				t.Fatal("changed source was accepted after reread")
			} else if kind, _ := errs.KindOf(err); kind != errs.KindStateConflict {
				t.Fatalf("changed source returned %v, want state conflict", err)
			}
			current, guards, err := ledger.matchRemoteReleaseSource(t.Context(), source, store.currentRevision())
			if err != nil || current {
				t.Fatalf("changed source could not settle as superseded: current=%t, err=%v", current, err)
			}
			result, err = store.Transact(t.Context(), guards, []keyvalue.Mutation{
				{Type: keyvalue.MutationPut, Key: "/test/superseded", Value: []byte("settled")},
			})
			if err != nil || !result.Succeeded {
				t.Fatalf("current supersession guards did not commit: succeeded=%t, err=%v", result.Succeeded, err)
			}
		})
	}
}
