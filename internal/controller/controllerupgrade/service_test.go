package controllerupgrade

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: actual protected idempotency and Task repositories must freeze one
// native input and replay its accepted Task even while recovery owns the host.
func TestNativeUpdateServicePublishesAndReplaysPinnedTask(t *testing.T) {
	h := newServiceHarness(t)
	ctx := context.Background()
	response, err := h.service.UpdateController(ctx, string(h.input.Release), "native-update-key-0001")
	if err != nil || response.Status != http.StatusAccepted {
		t.Fatalf("update = %s, %v", response, err)
	}
	var accepted apiTypes.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatal(err)
	}
	task, err := h.tasks.GetTask(ctx, accepted.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := DecodeTask(task.Record, task.Record.CreatedAt, task.Record.CreatedAt.Add(600*time.Second))
	if err != nil || journal.Release != h.input.Release || journal.PreviousController != h.input.PreviousController ||
		journal.Agent == nil || *journal.Agent != *h.input.Agent {
		t.Fatalf("frozen Task = %#v, %v", journal, err)
	}
	h.catalog.failure = errs.New(errs.KindResourceInUse, "host now upgrading")
	replay, err := h.service.UpdateController(ctx, string(h.input.Release), "native-update-key-0001")
	if err != nil || string(replay.Body) != string(response.Body) || h.storage.transactions != 1 {
		t.Fatalf("active replay = %s, %v; %d writes", replay, err, h.storage.transactions)
	}
	_, err = h.service.UpdateController(ctx, string(upgrade.Hash([]byte("other release"))), "native-update-key-0001")
	if !errors.Is(err, errs.New(errs.KindIdempotencyMismatch, "")) {
		t.Fatalf("changed release replay = %v", err)
	}
}

// Rationale: a lost publication response is not permission to create another
// host operation; pending durable acceptance resolves to the original Task.
func TestNativeUpdateServiceResolvesLostPublicationResponse(t *testing.T) {
	h := newServiceHarness(t)
	h.storage.loseResponse = true
	response, err := h.service.UpdateController(context.Background(), string(h.input.Release), "native-update-key-0002")
	if err != nil || response.Status != http.StatusAccepted || h.storage.transactions != 1 {
		t.Fatalf("unknown publication = %s, %v; %d writes", response, err, h.storage.transactions)
	}
}

// Rationale: foreground development remains usable without native bootstrap,
// but cannot accept native updates or silently choose an unqualified Agent image.
func TestNativeReleaseImageSelection(t *testing.T) {
	h := newServiceHarness(t)
	ctx := context.Background()
	image, err := h.service.DesiredAgentImage(ctx)
	if err != nil || image != h.input.Agent.Image {
		t.Fatalf("bootstrap image = %q, %v", image, err)
	}
	h.catalog.selected = upgrade.Release{Release: h.input.Release, Manifest: h.input.Manifest}
	h.catalog.hasSelected = true
	image, err = h.service.DesiredAgentImage(ctx)
	if err != nil || image != h.input.Manifest.AgentImage {
		t.Fatalf("qualified image = %q, %v", image, err)
	}
	h.catalog.failure = errs.New(errs.KindResourceInUse, "activation owns selection")
	if _, err := h.service.DesiredAgentImage(ctx); !errors.Is(err, h.catalog.failure) {
		t.Fatalf("active image = %v", err)
	}
	h.service.catalog = nil
	image, err = h.service.DesiredAgentImage(ctx)
	if err != nil || image != h.input.Agent.Image {
		t.Fatalf("development image = %q, %v", image, err)
	}
	if _, err := h.service.UpdateController(ctx, string(h.input.Release), "native-update-key-0003"); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("unbootstrapped update = %v", err)
	}
}

// Rationale: installed/running mismatch and an unready Agent must be rejected
// before a durable host-effect Task is published.
func TestNativeUpdateServiceRejectsInvalidPredecessor(t *testing.T) {
	for _, scenario := range []string{"process", "agent", "active"} {
		t.Run(scenario, func(t *testing.T) {
			h := newServiceHarness(t)
			switch scenario {
			case "process":
				h.catalog.installed = upgrade.Hash([]byte("other executable"))
			case "agent":
				h.agents.agent.Phase = "provisioning"
			case "active":
				h.catalog.journal, h.catalog.found = h.expected, true
			}
			if _, err := h.service.UpdateController(context.Background(), string(h.input.Release), "native-update-key-0004"); err == nil ||
				h.storage.transactions != 0 {
				t.Fatalf("invalid predecessor accepted: %v, %d writes", err, h.storage.transactions)
			}
		})
	}
}

type serviceHarness struct {
	*coordinatorHarness
	service *Service
	catalog *serviceCatalog
	storage *publicationStore
	tasks   *etcd.TaskRepository
}

func newServiceHarness(t *testing.T) *serviceHarness {
	t.Helper()
	h := newCoordinatorHarness(t)
	storage := &publicationStore{revision: 1, entries: make(map[string]etcd.KeyValue)}
	tasks, err := etcd.NewTaskRepository(storage)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := etcd.NewIdempotencyRepository(storage)
	if err != nil {
		t.Fatal(err)
	}
	protector, err := secretvalue.NewProtector(publicationCipher{}, publicationCipher{})
	if err != nil {
		t.Fatal(err)
	}
	intents, err := idempotentintent.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	catalog := &serviceCatalog{coordinatorStore: h.store}
	service, err := NewService(ServiceDependencies{Catalog: catalog, Unit: h.unit, Agents: h.agents,
		Tasks: tasks, Evidence: evidence, Intents: intents, ProcessDigest: h.input.PreviousController,
		BootstrapAgentImage: h.input.Agent.Image})
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return h.now }
	return &serviceHarness{coordinatorHarness: h, service: service, catalog: catalog, storage: storage, tasks: tasks}
}

type serviceCatalog struct {
	*coordinatorStore
	selected    upgrade.Release
	hasSelected bool
	failure     error
}

func (catalog *serviceCatalog) Current(ctx context.Context) (upgrade.Journal, bool, error) {
	if catalog.failure != nil {
		return upgrade.Journal{}, false, catalog.failure
	}
	return catalog.coordinatorStore.Current(ctx)
}
func (catalog *serviceCatalog) Selected(context.Context) (upgrade.Release, bool, error) {
	return catalog.selected, catalog.hasSelected, catalog.failure
}
func (*serviceCatalog) Candidate(context.Context) (upgrade.Release, bool, error) {
	return upgrade.Release{}, false, nil
}

type publicationCipher struct{}

func (publicationCipher) Seal(_ context.Context, raw []byte) ([]byte, error) {
	return append([]byte("test-sealed:"), raw...), nil
}
func (publicationCipher) Open(_ context.Context, raw []byte) ([]byte, error) {
	return append([]byte(nil), raw[len("test-sealed:"):]...), nil
}
