package desiredrevision

import (
	"context"
	"crypto/sha256"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type boundaryRepository struct {
	claims               int
	stages               int
	publications         int
	retainClaim          bool
	winner               *etcd.EnvironmentBlueprintStageClaim
	stagedTaskID         string
	publishedClaimTaskID string
	publishedTaskID      string
}

func (repository *boundaryRepository) ClaimEnvironmentBlueprintStage(
	_ context.Context,
	request etcd.EnvironmentBlueprintStageClaimRequest,
) (etcd.EnvironmentBlueprintStageClaim, error) {
	repository.claims++
	if repository.retainClaim && repository.winner != nil {
		winner := *repository.winner
		winner.Intent.Ciphertext = append([]byte(nil), winner.Intent.Ciphertext...)
		winner.Existing = true
		return winner, nil
	}
	claim := etcd.EnvironmentBlueprintStageClaim{
		DescriptorID:  strings.TrimPrefix(request.CandidateTaskID, "task_"),
		EnvironmentID: request.EnvironmentID, RevisionID: request.CandidateRevisionID,
		TaskID: request.CandidateTaskID, Locator: request.Locator, Intent: request.Intent,
		BaselineHeadRevision: request.BaselineHeadRevision, SourceKind: request.SourceKind,
		RenderGeneration: request.RenderGeneration, ProjectionSchema: request.ProjectionSchema,
		CreatedAt: request.CreatedAt,
	}
	if repository.retainClaim {
		winner := claim
		winner.Intent.Ciphertext = append([]byte(nil), claim.Intent.Ciphertext...)
		repository.winner = &winner
	}
	return claim, nil
}

func (repository *boundaryRepository) StageEnvironmentBlueprintRevision(
	_ context.Context,
	request etcd.EnvironmentBlueprintStageRequest,
) (etcd.EnvironmentBlueprintSeal, error) {
	repository.stages++
	repository.stagedTaskID = request.Claim.TaskID
	return etcd.EnvironmentBlueprintSeal{}, nil
}

func (repository *boundaryRepository) PublishEnvironmentDesiredRevisionWithTask(
	_ context.Context,
	_ etcd.Versioned[etcd.ProjectRecord],
	_ etcd.Versioned[etcd.EnvironmentRecord],
	_ int64,
	claim etcd.EnvironmentBlueprintStageClaim,
	_ etcd.EnvironmentDesiredRevisionIdentity,
	_ etcd.EnvironmentComposeProjection,
	_ []etcd.EnvironmentBlueprintZoneChange,
	_ []etcd.EnvironmentBlueprintServiceChange,
	_ []etcd.EnvironmentBlueprintRouteChange,
	_ etcd.ReleaseGroupBlueprintPreparedMutation,
	_ etcd.ComponentTaskPreparation,
	task etcd.TaskRecord,
	_ etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.publications++
	repository.publishedClaimTaskID = claim.TaskID
	repository.publishedTaskID = task.ID
	return etcd.IdempotencyTransactionResult{}, nil
}

func TestPreflightAndClaimEnforcesExactNormalizedProjectionBoundary(t *testing.T) {
	t.Parallel()
	const limit = uint64(2 * 1024 * 1024)
	now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	revisionID := ids.NewAt(ids.KindTask, now, 2)

	low, high := 1, int(limit)
	var exact etcd.EnvironmentComposeProjection
	var exactEvidence ProjectionEvidence
	for low <= high {
		middle := low + (high-low)/2
		candidate := boundaryProjection(t, now, environmentID, revisionID, middle, 0)
		evidence, err := PreflightProjection(candidate)
		if err != nil {
			high = middle - 1
			continue
		}
		exact, exactEvidence = candidate, evidence
		low = middle + 1
	}
	if exactEvidence.NormalizedBytes == 0 || exactEvidence.NormalizedBytes > limit {
		t.Fatalf("largest accepted normalized projection = %d", exactEvidence.NormalizedBytes)
	}
	padding := int(limit - exactEvidence.NormalizedBytes)
	exact.Routes[0].Path += strings.Repeat("a", padding)
	exactEvidence, err := PreflightProjection(exact)
	if err != nil || exactEvidence.NormalizedBytes != limit {
		t.Fatalf("exact normalized projection = %d, %v", exactEvidence.NormalizedBytes, err)
	}

	claimInput := boundaryClaimInput(now, environmentID, revisionID)
	accepted := &boundaryRepository{}
	claim, acceptedEvidence, err := PreflightAndClaim(
		context.Background(), accepted, exact, claimInput,
	)
	if err != nil || claim.TaskID != revisionID || acceptedEvidence.NormalizedBytes != limit ||
		accepted.claims != 1 || accepted.stages != 0 || accepted.publications != 0 {
		t.Fatalf("exact boundary authority = %#v/%#v/%d/%d/%d/%v",
			claim, acceptedEvidence, accepted.claims, accepted.stages, accepted.publications, err)
	}

	over := exact
	over.Routes = append([]etcd.EnvironmentRouteIdentity(nil), exact.Routes...)
	over.Routes[0].Path += "a"
	rejected := &boundaryRepository{}
	if _, evidence, err := PreflightAndClaim(
		context.Background(), rejected, over, claimInput,
	); err == nil || evidence != (ProjectionEvidence{}) || rejected.claims != 0 ||
		rejected.stages != 0 || rejected.publications != 0 {
		t.Fatalf("oversized normalized projection authority = %#v/%d/%d/%d/%v",
			evidence, rejected.claims, rejected.stages, rejected.publications, err)
	}
}

// Rationale: a crash after the durable claim but before publication must resume with the winner's Task authority and byte-identical Release Group identity allocation.
func TestBlueprintClaimCrashReplayResumesWithStableTaskAndReleaseGroupIDs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 8, 26, 21, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 20)
	firstCandidate := ids.NewAt(ids.KindTask, now, 21)
	repository := &boundaryRepository{retainClaim: true}

	firstInput := boundaryClaimInput(now, environmentID, firstCandidate)
	first, err := Claim(ctx, repository, firstInput)
	if err != nil {
		t.Fatalf("Claim(first) error = %v", err)
	}
	firstAllocator, err := NewBlueprintIdentityAllocator(first)
	if err != nil {
		t.Fatalf("NewBlueprintIdentityAllocator(first) error = %v", err)
	}
	firstGroupID := firstAllocator.New(ids.KindReleaseGroup)
	firstProjection := boundaryProjection(t, first.CreatedAt, environmentID, first.TaskID, 64, 0)
	firstProjectionEvidence, err := PreflightProjection(firstProjection)
	if err != nil {
		t.Fatalf("PreflightProjection(first) error = %v", err)
	}

	secondInput := firstInput
	secondInput.CandidateTaskID = ids.NewAt(ids.KindTask, now.Add(time.Second), 22)
	secondInput.CreatedAt = now.Add(time.Second)
	recovered, err := Claim(ctx, repository, secondInput)
	if err != nil {
		t.Fatalf("Claim(recovered) error = %v", err)
	}
	recoveredAllocator, err := NewBlueprintIdentityAllocator(recovered)
	if err != nil {
		t.Fatalf("NewBlueprintIdentityAllocator(recovered) error = %v", err)
	}
	recoveredGroupID := recoveredAllocator.New(ids.KindReleaseGroup)
	recoveredProjection := boundaryProjection(
		t, recovered.CreatedAt, environmentID, recovered.TaskID, 64, 0,
	)
	recoveredProjectionEvidence, err := PreflightProjection(recoveredProjection)
	if err != nil {
		t.Fatalf("PreflightProjection(recovered) error = %v", err)
	}
	if !recovered.Existing || first.TaskID == secondInput.CandidateTaskID ||
		recovered.TaskID != first.TaskID || recoveredGroupID != firstGroupID ||
		recoveredProjectionEvidence != firstProjectionEvidence {
		t.Fatalf(
			"recovered authority = existing:%t task:%q/%q group:%q/%q projection:%#v/%#v",
			recovered.Existing, recovered.TaskID, first.TaskID,
			recoveredGroupID, firstGroupID, recoveredProjectionEvidence, firstProjectionEvidence,
		)
	}

	if _, err := repository.StageEnvironmentBlueprintRevision(ctx, etcd.EnvironmentBlueprintStageRequest{
		Claim: recovered, Projection: recoveredProjection,
		DependencyDigest: recoveredProjectionEvidence.DependencyDigest,
	}); err != nil {
		t.Fatalf("StageEnvironmentBlueprintRevision(recovered) error = %v", err)
	}
	if _, err := repository.PublishEnvironmentDesiredRevisionWithTask(
		ctx,
		etcd.Versioned[etcd.ProjectRecord]{},
		etcd.Versioned[etcd.EnvironmentRecord]{},
		0,
		recovered,
		etcd.EnvironmentDesiredRevisionIdentity{EnvironmentID: environmentID, RevisionID: recovered.TaskID},
		recoveredProjection,
		nil,
		nil,
		nil,
		etcd.ReleaseGroupBlueprintPreparedMutation{},
		etcd.ComponentTaskPreparation{},
		etcd.TaskRecord{ID: recovered.TaskID},
		etcd.IdempotencyMarker{},
	); err != nil {
		t.Fatalf("PublishEnvironmentDesiredRevisionWithTask(recovered) error = %v", err)
	}
	if repository.claims != 2 || repository.stages != 1 || repository.publications != 1 ||
		repository.stagedTaskID != first.TaskID ||
		repository.publishedClaimTaskID != first.TaskID ||
		repository.publishedTaskID != first.TaskID {
		t.Fatalf(
			"resume calls = %d/%d/%d, Task authority = %q/%q/%q, want %q",
			repository.claims, repository.stages, repository.publications,
			repository.stagedTaskID, repository.publishedClaimTaskID,
			repository.publishedTaskID, first.TaskID,
		)
	}
}

func boundaryProjection(
	t *testing.T,
	now time.Time,
	environmentID string,
	revisionID string,
	yamlBytes int,
	pathPadding int,
) etcd.EnvironmentComposeProjection {
	t.Helper()
	canonicalYAML := []byte(strings.Repeat("x", yamlBytes))
	digest := sha256.Sum256(canonicalYAML)
	artifact, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: ids.NewAt(ids.KindConfig, now, 3),
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:    environmentID, ProjectName: "boundary",
		CanonicalYaml: canonicalYAML, YamlSha256: digest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/boundary",
	})
	if err != nil {
		t.Fatal(err)
	}
	return etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 1,
		ComposeArtifact: artifact,
		Routes: []etcd.EnvironmentRouteIdentity{{
			ID: ids.NewAt(ids.KindRoute, now, 4), Host: "boundary.example.test",
			Path: "/" + strings.Repeat("a", pathPadding),
		}},
	}
}

func boundaryClaimInput(now time.Time, environmentID, revisionID string) ClaimInput {
	ciphertext := []byte("protected-boundary-intent")
	digest := sha256.Sum256(ciphertext)
	intent := etcd.ProtectedIntentRecord{
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextDigest: stringDigest(digest), Ciphertext: ciphertext,
	}
	return ClaimInput{
		EnvironmentID: environmentID, CandidateTaskID: revisionID,
		Locator: etcd.IdempotencyLocator{
			ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
			Method: "PUT", Route: blueprintRoute, Key: "boundary-idempotency-key-0001",
		},
		Intent: intent,
		MatchExistingIntent: func(_ context.Context, existing etcd.ProtectedIntentRecord) (bool, error) {
			return reflect.DeepEqual(existing, intent), nil
		},
		SourceKind: etcd.EnvironmentBlueprintSourceApply, RenderGeneration: 1, CreatedAt: now,
	}
}

func stringDigest(value [sha256.Size]byte) string {
	const alphabet = "0123456789abcdef"
	result := make([]byte, sha256.Size*2)
	for index, octet := range value {
		result[index*2] = alphabet[octet>>4]
		result[index*2+1] = alphabet[octet&15]
	}
	return string(result)
}
