//go:build etcd_acceptance

package etcd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a circuit breaker that also fences recovery or cannot reopen after
// reclamation would make quota incidents permanent. Use an isolated real backend
// with a small quota; this does not qualify installed operation recovery.
func TestStorageReserveRealEtcdAdmissionAndReclamation(t *testing.T) {
	endpoint := os.Getenv("GP_STORAGE_ETCD_ENDPOINT")
	address, err := url.Parse(endpoint)
	if err != nil || address.Scheme != "http" || address.Hostname() != "127.0.0.1" || address.Port() == "" {
		t.Fatal("GP_STORAGE_ETCD_ENDPOINT must select an isolated loopback etcd with a 16 MiB quota")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	store, err := New(
		ctx,
		[]string{endpoint},
		"/storage-reserve-acceptance/",
		config.DefaultMaintenanceConfig(),
		t.TempDir(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	status, err := store.backend.Status(ctx, endpoint)
	if err != nil || status.DbSizeQuota != 16<<20 {
		t.Fatalf("requires disposable 16 MiB backend: status=%v error=%v", status, err)
	}
	if err := store.Maintenance().CheckNewMutation(ctx); err != nil {
		t.Fatal(err)
	}
	value := []byte(strings.Repeat("x", 512<<10))
	for index := 0; index < 24; index++ {
		if err := store.Maintenance().CheckNewMutation(ctx); err != nil {
			break
		}
		if _, err := store.Put(ctx, fmt.Sprintf("/pressure/%02d", index), value); err != nil {
			t.Fatal(err)
		}
	}
	if kind, _ := errs.KindOf(store.Maintenance().CheckNewMutation(ctx)); kind != errs.KindStorageUnavailable {
		t.Fatal("new mutations were not paused before backend quota")
	}
	at := time.Now().UTC()
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor,
		Action: workexecution.ActionApply, TargetID: ids.New(ids.KindEnvironment),
		Intent:    workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)},
		CreatedAt: at, UpdatedAt: at, Deadline: at.Add(time.Hour)}
	encoded, err := workqueue.EncodeTask(task)
	if err != nil {
		t.Fatal(err)
	}
	key := workqueue.TaskKey(task.ID)
	_, err = store.Transact(
		ctx,
		[]keyvalue.Condition{{Key: key}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: encoded}},
	)
	if kind, _ := errs.KindOf(err); kind != errs.KindStorageUnavailable {
		t.Fatalf("background acceptance was not paused: %v", err)
	}
	read, err := store.Get(ctx, key)
	if err != nil || read.Entry != nil {
		t.Fatal("rejected acceptance left a Task behind")
	}
	// Existing accepted records remain writable, including their checkpoint CAS.
	revision, err := store.Put(ctx, "/accepted-checkpoint", []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(ctx, []keyvalue.Condition{{Key: "/accepted-checkpoint", ModRevision: revision}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: "/accepted-checkpoint", Value: []byte("after")}})
	if err != nil || !result.Succeeded {
		t.Fatalf("accepted checkpoint blocked: %v", err)
	}
	task.Actor, task.Action, task.ExpectedItems = workexecution.SystemActor, workexecution.ActionBackupPrune, 1
	encoded, err = workqueue.EncodeTask(task)
	if err != nil {
		t.Fatal(err)
	}
	result, err = store.Transact(
		ctx,
		[]keyvalue.Condition{{Key: key}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: encoded}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("bounded system pruning blocked: %v", err)
	}
	result, err = store.Transact(
		ctx,
		nil,
		[]keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: "/pressure/", Prefix: true}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("cleanup blocked: %v", err)
	}
	if _, err := store.backend.Compact(ctx, result.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err := store.backend.Defragment(ctx, endpoint); err != nil {
		t.Fatal(err)
	}
	if err := store.Maintenance().CheckNewMutation(ctx); err != nil {
		t.Fatalf("admission did not reopen after reclamation: %v", err)
	}
}
