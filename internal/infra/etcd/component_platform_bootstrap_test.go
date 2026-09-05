package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: clean-start CoreDNS reconciliation is published before Agent
// enrollment and must remain claimable once an Agent later becomes Ready.
func TestPlatformResolverBootstrapTaskIsClaimable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	components, err := newComponentRepository(store)
	if err != nil {
		t.Fatalf("newComponentRepository() error = %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	records, err := DefaultPlatformComponents(false)
	if err != nil {
		t.Fatalf("DefaultPlatformComponents() error = %v", err)
	}
	created, err := components.EnsurePlatformComponents(ctx, records)
	if err != nil {
		t.Fatalf("EnsurePlatformComponents() error = %v", err)
	}
	current := created[0]
	projection, err := NewHostResolutionProjectionRecord(current.ReadRevision, nil)
	if err != nil {
		t.Fatalf("NewHostResolutionProjectionRecord() error = %v", err)
	}
	now := time.Date(2026, time.August, 31, 11, 59, 18, 0, time.UTC)
	task := newPlatformDNSResolverTask(current.Record.Desired.ID, now)
	task.IdempotencyKey = task.OperationID
	desiredSHA256, err := PlatformComponentDesiredDigest(current.Record)
	if err != nil {
		t.Fatalf("PlatformComponentDesiredDigest() error = %v", err)
	}
	serviceID := ids.NewAt(ids.KindService, now, 1)
	composeArtifactID := ids.NewAt(ids.KindConfig, now, 2)
	input := PlatformComponentTaskRenderInput{
		PlanID: task.PlanID, TaskID: task.ID, ComponentID: task.Target,
		DesiredSHA256: desiredSHA256, BaselineGeneration: 1, BaselineSHA256: strings.Repeat("a", 64),
		HostResolutionInputRevision: projection.InputRevision, HostResolutionSHA256: projection.InputSHA256,
		Config: *current.Record.Desired.Config.CoreDNS, GeneratedServiceID: serviceID,
		DefinitionSHA256: strings.Repeat("b", 64), CatalogSHA256: strings.Repeat("c", 64),
		ActionID: "activate-config", ArtifactID: ids.NewAt(ids.KindConfig, now, 3),
		ComposeArtifactID: composeArtifactID,
		ComposeArtifact: testPlatformComponentComposeArtifact(
			composeArtifactID, serviceID, strings.Repeat("9", 64),
		),
		ArtifactSHA256: strings.Repeat("d", 64), OwnershipPlanID: task.PlanID, OwnershipGeneration: 1,
		ImageRepository: "coredns/coredns", ImageIndexDigest: strings.Repeat("7", 64),
		ImageConfigDigest: strings.Repeat("9", 64),
		ImageChildDigest:  strings.Repeat("8", 64), ImageReference: "coredns/coredns@sha256:" + strings.Repeat("8", 64),
		ImageOS: "linux", ImageArchitecture: "amd64", ArtifactLength: 1,
		PlanSHA256: strings.Repeat("e", 64), ExecutionPlanSHA256: strings.Repeat("f", 64),
	}
	task.PlanHash = input.ExecutionPlanSHA256
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeKind = IdempotencyScopePlatform
	marker.Locator.ScopeID = "-"
	marker.Locator.Route = "/components/{id}/update"
	if err := tasks.PublishPlatformDNSResolverTask(ctx, current, projection, task, input, marker); err != nil {
		t.Fatalf("PublishPlatformDNSResolverTask() error = %v", err)
	}
	assignment, found, err := tasks.ClaimNextTask(
		ctx,
		ids.NewAt(ids.KindAgent, now, 4),
		1,
		now.Add(time.Second),
	)
	if err != nil || !found || assignment.Task.Record.ID != task.ID {
		t.Fatalf("ClaimNextTask() = %#v, %t, %v, want bootstrap Task", assignment, found, err)
	}
}

// Rationale: only the atomic singleton bootstrap transaction may authorize a
// missing-observation first activation, and any later record write invalidates
// that authority even when runtime fields still look empty.
func TestPlatformComponentBootstrapProvenanceIsRepositoryOwned(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, err := newComponentRepository(newMemoryTaskStore())
	if err != nil {
		t.Fatalf("NewComponentRepository() error = %v", err)
	}
	records, err := DefaultPlatformComponents(false)
	if err != nil {
		t.Fatalf("DefaultPlatformComponents() error = %v", err)
	}
	created, err := repository.EnsurePlatformComponents(ctx, records)
	if err != nil {
		t.Fatalf("EnsurePlatformComponents() error = %v", err)
	}
	found, err := repository.HasPlatformComponentBootstrapProvenance(ctx, created[0])
	if err != nil || !found {
		t.Fatalf("HasPlatformComponentBootstrapProvenance() = %t, %v, want true, nil", found, err)
	}

	updated, err := repository.ReplacePlatformRuntime(ctx, created[0], nil, "", false)
	if err != nil {
		t.Fatalf("ReplacePlatformRuntime() error = %v", err)
	}
	_, err = repository.HasPlatformComponentBootstrapProvenance(ctx, updated)
	kind, found := errs.KindOf(err)
	if !found || kind != errs.KindStateConflict {
		t.Fatalf("HasPlatformComponentBootstrapProvenance() error = %v, want stale state conflict", err)
	}

	directRepository, err := newComponentRepository(newMemoryTaskStore())
	if err != nil {
		t.Fatalf("NewComponentRepository(direct) error = %v", err)
	}
	direct, err := directRepository.CreatePlatformComponent(ctx, records[0])
	if err != nil {
		t.Fatalf("CreatePlatformComponent() error = %v", err)
	}
	found, err = directRepository.HasPlatformComponentBootstrapProvenance(ctx, direct)
	if err != nil || found {
		t.Fatalf("HasPlatformComponentBootstrapProvenance(direct) = %t, %v, want false, nil", found, err)
	}
}
