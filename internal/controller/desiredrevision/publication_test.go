package desiredrevision

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type boundaryRepository struct {
	claims       int
	stages       int
	publications int
}

func (repository *boundaryRepository) ClaimEnvironmentBlueprintStage(
	_ context.Context,
	request etcd.EnvironmentBlueprintStageClaimRequest,
) (etcd.EnvironmentBlueprintStageClaim, error) {
	repository.claims++
	return etcd.EnvironmentBlueprintStageClaim{
		DescriptorID:  strings.TrimPrefix(request.CandidateTaskID, "task_"),
		EnvironmentID: request.EnvironmentID, RevisionID: request.CandidateRevisionID,
		TaskID: request.CandidateTaskID, Locator: request.Locator, Intent: request.Intent,
		BaselineHeadRevision: request.BaselineHeadRevision, SourceKind: request.SourceKind,
		RenderGeneration: request.RenderGeneration, ProjectionSchema: request.ProjectionSchema,
		CreatedAt: request.CreatedAt,
	}, nil
}

func (repository *boundaryRepository) StageEnvironmentBlueprintRevision(
	context.Context,
	etcd.EnvironmentBlueprintStageRequest,
) (etcd.EnvironmentBlueprintSeal, error) {
	repository.stages++
	return etcd.EnvironmentBlueprintSeal{}, nil
}

func (repository *boundaryRepository) PublishEnvironmentDesiredRevisionWithTask(
	context.Context,
	etcd.Versioned[etcd.ProjectRecord],
	etcd.Versioned[etcd.EnvironmentRecord],
	int64,
	etcd.EnvironmentBlueprintStageClaim,
	etcd.EnvironmentDesiredRevisionIdentity,
	etcd.EnvironmentComposeProjection,
	etcd.TaskRecord,
	etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.publications++
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
	return ClaimInput{
		EnvironmentID: environmentID, CandidateTaskID: revisionID,
		Locator: etcd.IdempotencyLocator{
			ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
			Method: "PUT", Route: blueprintRoute, Key: "boundary-idempotency-key-0001",
		},
		Intent: etcd.ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: stringDigest(digest), Ciphertext: ciphertext,
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
