package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/blueprint"
	entryoperations "github.com/AlanD20/groundplane/internal/controller/entry/operations"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-79: exercise the active desired-state publisher, not the retired direct
// Entry writer. An edit preserves file identity/ownership, seals a new secret
// generation and redacts durable metadata/responses. A pending duplicate reports
// its existing work without staging another value generation.
// This supplies an empty runtime; it does not qualify Agent file application.
func TestNativeEntryEditPreservesShapeAndProtectsValue(t *testing.T) {
	f := newNativeVolumeFixture(t)
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
	desired, err := desiredrevision.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	zones, err := etcd.NewZoneRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	services, err := etcd.NewServiceRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := etcd.NewRouteRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := etcd.NewEntryRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	values, err := entryvalues.New(f.store)
	if err != nil {
		t.Fatal(err)
	}
	attaches, err := etcd.NewAttachRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	components, err := etcd.NewComponentRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	scripts, err := etcd.NewScriptRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := blueprint.NewRepository(f.blueprint, desired, zones, services,
		routes, entries, values, attaches, components, scripts, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	secrets, err := etcd.NewSecretRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	generator, err := entrygeneration.NewEntryGenerationService(secrets, nativeEntryNoFacts{}, protector)
	if err != nil {
		t.Fatal(err)
	}
	planner, err := taskplanning.NewTaskPlanResolver("/var/lib/groundplane/vol", nil)
	if err != nil {
		t.Fatal(err)
	}
	materials, err := initializeTaskMaterializationResolver(f.store, f.blueprint.HierarchyRepository,
		values, secrets, planner, protector)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := etcd.NewIdempotencyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	creation, err := entryoperations.NewCreationIdempotency(coordinator, replay)
	if err != nil {
		t.Fatal(err)
	}
	edit, err := entryoperations.NewEditIdempotency(coordinator, replay)
	if err != nil {
		t.Fatal(err)
	}
	removal, err := entryoperations.NewRemovalIdempotency(coordinator, replay)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := etcd.NewTaskRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := etcd.NewReleaseLedger(f.store, tasks)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := taskplanning.NewEntryRuntimeCapture(ledger)
	if err != nil {
		t.Fatal(err)
	}
	mutations, err := entryoperations.NewDesiredMutationService(repository, generator,
		materials, creation, edit, removal, runtime, f.blueprint.HierarchyRepository)
	if err != nil {
		t.Fatal(err)
	}
	uid, gid := int64(1000), int64(1001)
	created, err := mutations.CreateEntry(t.Context(), api.EntryCreateRequest{
		EnvironmentID: f.environmentID, Type: "file", Path: "config/private.ini",
		UID: &uid, GID: &gid, Secret: true, Exposure: []string{"all"},
		Source: api.EntrySource{Kind: "literal", Literal: "original-private-value"},
	}, "native-entry-create-value")
	if err != nil {
		t.Fatal(err)
	}
	var original api.Entry
	if err := json.Unmarshal(created.Body, &original); err != nil || original.ID == "" {
		t.Fatalf("create Entry response: %v", err)
	}
	before, found, err := f.blueprint.GetEnvironmentComposeProjection(t.Context(), f.environmentID)
	if err != nil || !found || len(before.Record.Entries) != 1 {
		t.Fatalf("created Entry: %v", err)
	}
	previous := before.Record.Entries[0]
	input := api.EntryEditRequest{
		Source:   api.EntrySource{Kind: "literal", Literal: "replacement-private-value"},
		Exposure: []string{"all"},
	}
	response, err := mutations.EditEntry(t.Context(), original.ID, input, "native-entry-edit-value")
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != http.StatusOK || bytes.Contains(response.Body, []byte(input.Source.Literal)) {
		t.Fatal("edit exposed private input or returned the wrong status")
	}
	after, found, err := f.blueprint.GetEnvironmentComposeProjection(t.Context(), f.environmentID)
	if err != nil || !found || len(after.Record.Entries) != 1 {
		t.Fatalf("edited Entry: %v", err)
	}
	current := after.Record.Entries[0]
	expected := previous.Entry
	expected.Source = core.EntrySource{Kind: core.SourceLiteral}
	if !reflect.DeepEqual(current.Entry, expected) || current.BlueprintKey != previous.BlueprintKey ||
		current.CurrentValueGenerationID == previous.CurrentValueGenerationID {
		t.Fatal("edit changed immutable Entry shape, leaked literal metadata or reused its generation")
	}
	protected, found, err := values.GetSecret(t.Context(), original.ID, current.CurrentValueGenerationID)
	if err != nil || !found {
		t.Fatalf("encrypted edit generation: %v", err)
	}
	defer clear(protected.Ciphertext)
	opened, err := cipher.Open(t.Context(), protected.Ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(opened)
	if string(opened) != input.Source.Literal {
		t.Fatal("edit encrypted different value bytes")
	}
	plain, found, err := values.GetPlain(t.Context(), original.ID, current.CurrentValueGenerationID)
	clear(plain.Content)
	if err != nil || found {
		t.Fatal("edit also persisted an unencrypted generation")
	}
	locator := idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID: f.environmentID, Method: http.MethodPatch, Route: "/entries/{id}", Key: "native-entry-edit-value"}
	evidence, err := replay.Read(t.Context(), locator)
	if err != nil || evidence == nil {
		t.Fatalf("edit replay lookup: %v", err)
	}
	marker, err := evidence.Marker()
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if err != nil || marker.ReplayTarget == nil || marker.ReplayTarget.ID != original.ID ||
		!bytes.Equal(marker.Response.Body, response.Body) {
		t.Fatalf("edit replay evidence: %v", err)
	}
	revision := f.store.revision
	_, err = mutations.EditEntry(t.Context(), original.ID, input, locator.Key)
	if !errors.Is(err, errs.New(errs.KindIdempotencyInProgress, "")) || f.store.revision != revision {
		t.Fatalf("pending edit replay changed state or did not identify active work: %v", err)
	}
	input.Source.Literal = "different-private-input"
	if _, err := mutations.EditEntry(t.Context(), original.ID, input, locator.Key); !errors.Is(
		err,
		errs.New(errs.KindIdempotencyMismatch, ""),
	) {
		t.Fatalf("changed edit replay accepted: %v", err)
	}
}

type nativeEntryNoFacts struct{}

func (nativeEntryNoFacts) ResolveFact(
	context.Context,
	string,
	core.FactRef,
	bool,
	secretvalue.PlaintextConsumer,
) error {
	return errs.New(errs.KindInternal, "literal Entry unexpectedly requested an Attach fact")
}
