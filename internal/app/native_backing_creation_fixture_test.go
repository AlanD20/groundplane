package app

import (
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/app/adaptercompiler"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/controller/backingservices"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingprovisionwork"
	desiredstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
)

// Reuses actual creation, encryption and publication owners. Only registry
// lookup and Agent-local image observations are supplied by the fixture.
func newNativeBackingCreationService(
	t *testing.T,
	f *nativeVolumeFixture,
) (*backingservices.CreationService, *nativeBackingCreationImages, *backingprovisionwork.Repository) {
	t.Helper()
	adaptercompiler.Register()
	desired, err := desiredstore.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := backingprovisionwork.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ageinfra.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	cipher := nativeVolumeCipher{key: key}
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := etcd.NewIdempotencyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := desiredrevision.NewIdempotency(coordinator, replay)
	if err != nil {
		t.Fatal(err)
	}
	agent, err := f.agents.GetSingleton(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	images := &nativeBackingCreationImages{t: t, agentID: agent.Record.ID, plan: imagefetch.Plan{
		Requested: "registry.example/internal/search:1.4", Repository: "registry.example/internal/search",
		ManifestDigest: "sha256:" + strings.Repeat("a", 64), ConfigDigest: "sha256:" + strings.Repeat("b", 64),
		Architecture: "amd64"}}
	accepted, err := newAcceptedActions(f.store, protector, f.queue, replay, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	service, err := backingservices.NewCreationService(
		"/var/lib/groundplane/vol",
		netip.MustParsePrefix("10.0.0.0/8"),
		nativeDesiredRepository{
			f.blueprint,
			desired,
		},
		idempotency,
		protector,
		inputs,
		nil,
		nil,
		f.agents,
		images,
		images,
		accepted,
	)
	if err != nil {
		t.Fatal(err)
	}
	preparations, err := taskdependencies.NewPreparations(f.queue, service.AcceptedPreparations())
	if err != nil {
		t.Fatal(err)
	}
	backings, err := workqueue.NewBackingCreationPublisher(f.store)
	if err != nil {
		t.Fatal(err)
	}
	preparations.ConfigureBackingCreation(backings)
	f.backingPreparations = preparations
	return service, images, inputs
}

type nativeBackingCreated struct {
	BackingService api.BackingService
	TaskID         string
}

func nativeBackingCreationResult(
	t *testing.T,
	f *nativeVolumeFixture,
	response idempotency.IdempotencyResponse,
) nativeBackingCreated {
	t.Helper()
	var accepted api.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil || accepted.TaskID == "" {
		t.Fatalf("Backing acceptance: %s / %v", response.Body, err)
	}
	for step := 0; step < 8; step++ {
		task, _, err := f.queue.ReadTask(t.Context(), accepted.TaskID, 0)
		if err != nil {
			t.Fatal(err)
		}
		if task.Task.FinishedAt != nil {
			failure, _ := f.queue.ReadAcceptedPreparationFailure(t.Context(), task.Task.ID, 0)
			t.Fatalf("Backing preparation failed: %+v / %+v", task.Task, failure)
		}
		if !task.Task.PendingPreparation() {
			repository, err := etcd.NewServiceRepository(f.store)
			if err != nil {
				t.Fatal(err)
			}
			service, err := repository.GetService(t.Context(), task.Task.TargetID)
			if err != nil {
				t.Fatal(err)
			}
			return nativeBackingCreated{
				TaskID: accepted.TaskID,
				BackingService: api.BackingService{ProjectID: task.Task.Scope.ProjectID,
					EnvironmentID: task.Task.Scope.EnvironmentID, ServiceID: task.Task.TargetID, BackingNetworkID: service.Record.BackingNetworkID,
					Authentication: string(service.Record.Desired.Authentication)},
			}
		}
		if _, err := f.backingPreparations.Advance(t.Context(), accepted.TaskID, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("Backing preparation did not finish within bound")
	return nativeBackingCreated{}
}
