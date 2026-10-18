package app

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/handlers"
	"github.com/AlanD20/groundplane/internal/controller/taskinspection"
	"github.com/AlanD20/groundplane/internal/controller/volume"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/etcdconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type nativeTaskInspectionFixture struct {
	volume      *nativeVolumeFixture
	server      *handlers.Server
	diagnostics *executiondiagnostics.Repository
	fault       *nativeTaskDiagnosticStore
}

// Only diagnostic reads can fail here. Execution, resource and replay reads
// continue through their actual repositories over the same MVCC store.
type nativeTaskDiagnosticStore struct {
	*nativeVolumeStore
	eventKey string
	fail     atomic.Bool
}

func (store *nativeTaskDiagnosticStore) GetMany(
	ctx context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	if store.fail.Load() && len(request.Keys) == 1 && request.Keys[0] == store.eventKey {
		return nil, errs.New(errs.KindInternal, "private diagnostic failure detail")
	}
	return store.nativeVolumeStore.GetMany(ctx, request)
}

func newNativeTaskInspectionFixture(t *testing.T, logs io.Writer) *nativeTaskInspectionFixture {
	t.Helper()
	f := &nativeTaskInspectionFixture{volume: newNativeVolumeFixture(t)}
	store, queue := f.volume.store, f.volume.queue
	f.fault = &nativeTaskDiagnosticStore{
		nativeVolumeStore: store,
		eventKey:          executiondiagnostics.TaskEventsKey(f.volume.created.TaskID),
	}
	var err error
	f.diagnostics, err = executiondiagnostics.New(f.fault, queue)
	if err != nil {
		t.Fatal(err)
	}
	images, err := imageoperations.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	hierarchy, err := etcd.NewHierarchyDeletionRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	components, err := componentwork.NewInputRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	runners, err := runnerwork.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	etcdConfig, err := etcdconfiguration.New(store)
	if err != nil {
		t.Fatal(err)
	}
	preparations, err := softwarepreparation.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := controllersoftware.New(store)
	if err != nil {
		t.Fatal(err)
	}
	agents, err := agentoperations.New(store)
	if err != nil {
		t.Fatal(err)
	}
	activations, err := softwareactivation.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	environments, err := environmentprovision.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := volume.NewAgentWork(f.volume.inputs, queue)
	if err != nil {
		t.Fatal(err)
	}
	retries, err := workdispatch.NewRetries(
		queue,
		[]workdispatch.RetryRegistration{{Operation: workexecution.Volume, Owner: owner}},
	)
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := taskinspection.New(queue, images, f.diagnostics, retries, hierarchy, components,
		runners, etcdConfig, preparations, controller, agents, activations, environments)
	if err != nil {
		t.Fatal(err)
	}
	f.server = handlers.New(
		store,
		slog.New(slog.NewTextHandler(logs, nil)),
		handlers.Options{TaskInspection: inspection},
	)
	return f
}

func (f *nativeTaskInspectionFixture) finish(t *testing.T, view workqueue.ResumeView) {
	t.Helper()
	progress := f.volume.progress(t, view)
	for progress.Stage != "complete" {
		progress = f.volume.advance(t, view, progress)
	}
	f.volume.complete(t, view, progress)
	task, _, err := f.volume.queue.ReadTask(t.Context(), view.Work.Task.Task.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	change, err := f.volume.queue.PrepareTaskFinish(t.Context(), task, at, at.Add(workexecution.TaskHistoryRetention))
	if err != nil {
		t.Fatal(err)
	}
	f.volume.commit(t, change)
}
