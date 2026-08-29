package etcd

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: persisted Runner desired state must use the one canonical GitHub,
// label, image, and derived runtime-name representation.
func TestRunnerDesiredCanonicalization(t *testing.T) {
	t.Parallel()
	desired := runnerTestDesired(200, RunnerOwnerProject,
		ids.NewAt(ids.KindProject, taskJournalTime(), 200),
		ids.NewAt(ids.KindTenant, taskJournalTime(), 201),
	)
	desired.GitHubURL = "https://github.com/AlanD20/Ground_Plane/"
	desired.Labels = []string{"Zeta", "alpha.two"}
	desired.ImageRef = "localhost:5000/groundplane-runner@sha256:" + strings.Repeat("a", 64)
	normalized, err := NormalizeRunnerDesired(desired)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.GitHubURL != "https://github.com/aland20/ground_plane" ||
		!slices.Equal(normalized.Labels, []string{"alpha.two", "zeta"}) {
		t.Fatalf("normalized desired = %#v", normalized)
	}
	name, err := RunnerName(desired.ID)
	if err != nil || name != "gp-"+strings.ToLower(strings.TrimPrefix(desired.ID, "run_")) {
		t.Fatalf("RunnerName() = %q, %v", name, err)
	}

	for _, mutate := range []func(*RunnerDesiredRecord){
		func(value *RunnerDesiredRecord) { value.GitHubURL = "https://github.com/aland20/groundplane.git" },
		func(value *RunnerDesiredRecord) { value.GitHubURL += "//" },
		func(value *RunnerDesiredRecord) { value.Labels = []string{"Linux"} },
		func(value *RunnerDesiredRecord) { value.Labels = []string{"Build", "build"} },
		func(value *RunnerDesiredRecord) { value.ImageRef = "ghcr.io/aland20/groundplane-runner:latest" },
	} {
		candidate := desired
		mutate(&candidate)
		if _, err := NormalizeRunnerDesired(candidate); err == nil {
			t.Fatalf("invalid Runner desired state passed: %#v", candidate)
		}
	}
}

// Rationale: Tenant slug uniqueness spans direct and Project ownership, and a
// successful replacement must preserve exact replay after the Runner is gone.
func TestRunnerTenantSlugIndexAndReplacementReplay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, repository, tenantID, projectID := newRunnerRepositoryFixture(t)
	config := runnerTestAllocationConfig()
	first := runnerTestDesired(210, RunnerOwnerTenant, tenantID, tenantID)
	first.Slug = "shared"
	firstTask := runnerTestTask(first, TaskCreate, 211, "runner-slug-create-0001")
	if result, err := repository.CreateRunnerWithTask(
		ctx,
		config,
		first,
		firstTask,
		runnerTestMarker(firstTask, first),
	); err != nil {
		t.Fatal(err)
	} else if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("first create = %v, %v, %v", outcome, conflict, classifyErr)
	}
	resolved, err := repository.ResolveRunner(ctx, tenantID, "shared")
	if err != nil || resolved.Record.Desired.ID != first.ID {
		t.Fatalf("ResolveRunner(shared) = %#v, %v", resolved, err)
	}

	second := runnerTestDesired(212, RunnerOwnerProject, projectID, tenantID)
	second.Slug = "shared"
	secondTask := runnerTestTask(second, TaskCreate, 213, "runner-slug-create-0002")
	result, err := repository.CreateRunnerWithTask(
		ctx,
		config,
		second,
		secondTask,
		runnerTestMarker(secondTask, second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil ||
		outcome != IdempotencyKnownConflict ||
		!isKind(conflict, errs.KindRunnerSlugConflict) {
		t.Fatalf("slug collision = %v, %v, %v", outcome, conflict, classifyErr)
	}

	ready := cloneRunnerRecord(resolved.Record)
	ready.ProvisioningState = RunnerProvisioningReady
	readyValue, err := encodeRunnerLifecycleRecord(ready.RunnerLifecycleRecord)
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := store.Transact(ctx,
		[]Condition{{Key: runnerLifecycleKey(first.ID), ModRevision: resolved.Record.LifecycleRevision}},
		[]Mutation{{Type: MutationPut, Key: runnerLifecycleKey(first.ID), Value: readyValue}},
	)
	clear(readyValue)
	if err != nil || !transaction.Succeeded {
		t.Fatalf("seed ready Runner = %#v, %v", transaction, err)
	}
	marker := testDirectMarker()
	marker.Locator.ScopeID = tenantID
	marker.Locator.Method = http.MethodPatch
	marker.Locator.Route = "/runners/{id}"
	marker.Locator.Key = "01ARZ3NDEKTSV4RRFFQ69G5FAA"
	marker.ReplayTarget = &IdempotencyReplayTarget{Kind: IdempotencyReplayTargetRunner, ID: first.ID}
	marker.Response.Body = []byte(`{"id":"` + first.ID + `","slug":"renamed"}`)
	result, err = repository.ReplaceRunnerSlugIdempotent(ctx, first.ID, "renamed", marker)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("replace slug = %v, %v, %v", outcome, conflict, classifyErr)
	}
	if _, err := repository.ResolveRunner(ctx, tenantID, "shared"); !isKind(err, errs.KindRunnerNotFound) {
		t.Fatalf("old slug resolve error = %v", err)
	}
	renamed, err := repository.ResolveRunner(ctx, tenantID, "renamed")
	if err != nil || renamed.Record.Desired.ID != first.ID {
		t.Fatalf("new slug resolve = %#v, %v", renamed, err)
	}
	removed, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationDelete, Key: runnerTenantSlugKey(tenantID, "renamed")},
		{Type: MutationDelete, Key: runnerLifecycleKey(first.ID)},
		{Type: MutationDelete, Key: runnerKey(first.ID)},
	})
	if err != nil || !removed.Succeeded {
		t.Fatalf("remove Runner = %#v, %v", removed, err)
	}
	replay, err := repository.ReplaceRunnerSlugIdempotent(ctx, first.ID, "renamed", marker)
	if err != nil {
		t.Fatal(err)
	}
	outcome, existing, conflict, classifyErr := replay.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownExisting ||
		!slices.Equal(existing.Response.Body, marker.Response.Body) {
		t.Fatalf("replacement replay = %v, %#v, %v, %v", outcome, existing.Response, conflict, classifyErr)
	}
}

// Rationale: pre-ADR scaffold records lack required identity and image fields
// and must be rejected instead of entering a compatibility dual-read path.
func TestRunnerScaffoldRecordIsRejected(t *testing.T) {
	t.Parallel()
	type scaffoldDesired struct {
		ID        string          `json:"id"`
		OwnerKind RunnerOwnerKind `json:"owner_kind"`
		OwnerID   string          `json:"owner_id"`
		TenantID  string          `json:"tenant_id"`
	}
	tenantID := ids.NewAt(ids.KindTenant, taskJournalTime(), 220)
	value, err := encodeEnvelope("runner", struct {
		Desired scaffoldDesired `json:"desired"`
	}{Desired: scaffoldDesired{
		ID: ids.NewAt(ids.KindRunner, taskJournalTime(), 221), OwnerKind: RunnerOwnerTenant,
		OwnerID: tenantID, TenantID: tenantID,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeRunnerDesiredRecord(value); !isKind(err, errs.KindInternal) {
		t.Fatalf("decode scaffold Runner error = %v", err)
	}
}
