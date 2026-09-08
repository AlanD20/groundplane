//go:build etcd_acceptance

package hierarchydeletion

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	etcdinfra "github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the adapter must replay the original accepted Task after a
// successor retry, and must reject a durable Task whose marker owner changed.
func TestEtcdRepositoryReplayAfterRetryRejectsCorruptTaskLocator(t *testing.T) {
	ctx := context.Background()
	endpoint := etcdAcceptanceEndpoint(t)
	now := time.Date(2026, 8, 30, 14, 0, 0, 0, time.UTC)
	store := etcdAcceptanceStore(t, ctx, endpoint, "/groundplane-hierarchy-controller-replay/")
	tenantID := ids.NewAt(ids.KindTenant, now, 901)
	otherTenantID := ids.NewAt(ids.KindTenant, now, 902)
	hierarchy, err := etcdinfra.NewHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hierarchy.CreateTenant(ctx, etcdinfra.TenantRecord{
		ID: tenantID, Slug: "controller-replay", Name: "Controller Replay",
	}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	journal, err := etcdinfra.NewHierarchyDeletionRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := etcdinfra.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	protector, err := secretvalue.NewProtector(controllerReplayCipher{}, controllerReplayCipher{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := idempotentintent.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := NewEtcdRepository(journal, idempotency, coordinator, fixedClock{now: now})
	if err != nil {
		t.Fatal(err)
	}
	taskID := ids.NewAt(ids.KindTask, now, 903)
	service := NewService(repository, controllerReplayExecutor{}, fixedIDs{task: taskID}, fixedClock{now: now})
	request := DeleteRequest{
		TargetKind:     TargetTenant,
		TargetID:       tenantID,
		IdempotencyKey: "controller-hierarchy-replay-0001",
	}
	accepted, err := service.Delete(ctx, request)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := service.Execute(ctx, accepted.TaskID); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	tasks, err := etcdinfra.NewTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	claim, found, err := tasks.ClaimNextControllerTask(ctx, now.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != accepted.TaskID {
		t.Fatalf("ClaimNextControllerTask() = %#v/%t/%v", claim, found, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, accepted.TaskID, etcdinfra.TaskStatusFailed, now.Add(2*time.Second)); err != nil {
		t.Fatalf("AcknowledgeControllerTask() error = %v", err)
	}
	retryID := ids.NewAt(ids.KindTask, now, 904)
	retryMarker := controllerReplayRetryMarker(
		tenantID,
		retryID,
		now.Add(3*time.Second),
		"controller-hierarchy-retry-0001",
	)
	retry, err := tasks.RetryTask(ctx, accepted.TaskID, retryID, etcdinfra.TaskActorOperator, retryMarker)
	if err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	if outcome, _, conflict, classifyErr := retry.Classify(); classifyErr != nil || conflict != nil ||
		outcome != etcdinfra.IdempotencyKnownApplied {
		t.Fatalf("RetryTask() classification = %v/%v/%v", outcome, conflict, classifyErr)
	}
	replayed, found, err := repository.ResolveDeletionReplay(ctx, request)
	if err != nil || !found || replayed.Operation.TaskID != accepted.TaskID ||
		replayed.Operation.ID != accepted.OperationID {
		t.Fatalf("ResolveDeletionReplay() = %#v/%t/%v, want original Task", replayed, found, err)
	}

	taskKey := "/v1/tasks/" + accepted.TaskID
	stored, err := store.Get(ctx, taskKey)
	if err != nil || stored == nil || stored.Entry == nil {
		t.Fatalf("Get(root Task) = %#v/%v", stored, err)
	}
	needle := []byte(`"scope_id":"` + tenantID + `"`)
	replacement := []byte(`"scope_id":"` + otherTenantID + `"`)
	corrupt := bytes.Replace(stored.Entry.Value, needle, replacement, 1)
	if bytes.Equal(corrupt, stored.Entry.Value) {
		t.Fatal("root Task marker locator was not changed")
	}
	if result, err := store.Transact(ctx, []etcdinfra.Condition{{Key: taskKey, ModRevision: stored.Entry.ModRevision}}, []etcdinfra.Mutation{{Type: etcdinfra.MutationPut, Key: taskKey, Value: corrupt}}); err != nil ||
		!result.Succeeded {
		t.Fatalf("corrupt Task locator write = %#v/%v", result, err)
	}
	_, _, replayErr := repository.ResolveDeletionReplay(ctx, request)
	if kind, ok := errs.KindOf(replayErr); !ok || kind != errs.KindInternal {
		t.Fatalf("ResolveDeletionReplay(corrupt Task locator) = %v, want internal", replayErr)
	}
}

// Rationale: two Project deletions under one Tenant share the marker scope,
// but the same key must be rejected as an intent mismatch rather than reported
// as missing reverse-index corruption.
func TestEtcdRepositoryRejectsSameTenantDifferentProjectReplay(t *testing.T) {
	ctx := context.Background()
	endpoint := etcdAcceptanceEndpoint(t)
	now := time.Date(2026, 8, 30, 15, 0, 0, 0, time.UTC)
	store := etcdAcceptanceStore(t, ctx, endpoint, "/groundplane-hierarchy-project-mismatch/")
	tenantID := ids.NewAt(ids.KindTenant, now, 911)
	projectOneID := ids.NewAt(ids.KindProject, now, 912)
	projectTwoID := ids.NewAt(ids.KindProject, now, 913)
	hierarchy, err := etcdinfra.NewHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hierarchy.CreateTenant(ctx, etcdinfra.TenantRecord{
		ID: tenantID, Slug: "project-replay", Name: "Project Replay",
	}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	for _, project := range []etcdinfra.ProjectRecord{
		{ID: projectOneID, TenantID: tenantID, Slug: "one", Name: "One", Kind: etcdinfra.ProjectKindTenant},
		{ID: projectTwoID, TenantID: tenantID, Slug: "two", Name: "Two", Kind: etcdinfra.ProjectKindTenant},
	} {
		if _, err := hierarchy.CreateProject(ctx, project); err != nil {
			t.Fatalf("CreateProject(%s) error = %v", project.ID, err)
		}
	}
	journal, err := etcdinfra.NewHierarchyDeletionRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := etcdinfra.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	protector, err := secretvalue.NewProtector(controllerReplayCipher{}, controllerReplayCipher{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := idempotentintent.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := NewEtcdRepository(journal, idempotency, coordinator, fixedClock{now: now})
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(
		repository,
		controllerReplayExecutor{},
		fixedIDs{task: ids.NewAt(ids.KindTask, now, 914)},
		fixedClock{now: now},
	)
	key := "controller-project-replay-0001"
	if _, err := service.Delete(ctx, DeleteRequest{
		TargetKind: TargetProject, TargetID: projectOneID, IdempotencyKey: key,
	}); err != nil {
		t.Fatalf("Delete(first Project) error = %v", err)
	}
	_, err = service.Delete(ctx, DeleteRequest{
		TargetKind: TargetProject, TargetID: projectTwoID, IdempotencyKey: key,
	})
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindIdempotencyMismatch {
		t.Fatalf("Delete(second Project) error = %v, want idempotency mismatch", err)
	}
}

type controllerReplayExecutor struct{}

func (controllerReplayExecutor) Execute(context.Context, Operation, Action) (AgentTerminalProof, error) {
	return AgentTerminalProof{}, errs.New(errs.KindInternal, "unexpected Agent action in tenant replay acceptance")
}

type controllerReplayCipher struct{}

func (controllerReplayCipher) Seal(_ context.Context, plaintext []byte) ([]byte, error) {
	return append([]byte(nil), plaintext...), nil
}

func (controllerReplayCipher) Open(_ context.Context, ciphertext []byte) ([]byte, error) {
	return append([]byte(nil), ciphertext...), nil
}

func controllerReplayRetryMarker(tenantID, taskID string, at time.Time, key string) etcdinfra.IdempotencyMarker {
	ciphertext := []byte("controller-retry-intent")
	digest := sha256.Sum256(ciphertext)
	body, _ := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: taskID})
	return etcdinfra.IdempotencyMarker{
		Kind: etcdinfra.IdempotencyMarkerTask, State: etcdinfra.IdempotencyMarkerPending,
		Locator: etcdinfra.IdempotencyLocator{
			ScopeKind: etcdinfra.IdempotencyScopeTenant, ScopeID: tenantID,
			Method: http.MethodPost, Route: "/tasks/{id}/retry", Key: key,
		},
		Intent: etcdinfra.ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
		},
		Response: etcdinfra.IdempotencyResponse{
			Status:      http.StatusAccepted,
			ContentKind: "application/json",
			Body:        body,
		},
		TaskID: taskID, CreatedAt: at, UpdatedAt: at,
	}
}

func etcdAcceptanceEndpoint(t *testing.T) string {
	t.Helper()
	endpoint := os.Getenv("GROUNDPLANE_TEST_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("GROUNDPLANE_TEST_ETCD_ENDPOINT is required")
	}
	return endpoint
}

func etcdAcceptanceStore(t *testing.T, ctx context.Context, endpoint, prefix string) etcdinfra.Store {
	t.Helper()
	store, err := etcdinfra.New(
		ctx,
		[]string{endpoint},
		prefix+time.Now().UTC().Format("20060102T150405.000000000")+"/",
	)
	if err != nil {
		t.Fatalf("New(real etcd) error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

var _ secretvalue.Sealer = controllerReplayCipher{}
var _ secretvalue.Opener = controllerReplayCipher{}
