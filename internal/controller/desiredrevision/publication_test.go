package desiredrevision

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprintplanning "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleasegroups "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type boundaryRepository struct {
	claims                int
	stages                int
	publications          int
	retainClaim           bool
	winner                *testblueprints.EnvironmentBlueprintStageClaim
	stagedTaskID          string
	publishedClaimTaskID  string
	publishedTaskID       string
	abandonments          int
	abandonedLocator      testidempotency.IdempotencyLocator
	abandonmentContextErr error
	publicationErr        error
	publicationStarted    chan struct{}
	publicationRelease    chan struct{}
}

func (repository *boundaryRepository) AbandonEnvironmentBlueprintStage(
	ctx context.Context,
	claim testblueprints.EnvironmentBlueprintStageClaim,
) error {
	repository.abandonmentContextErr = ctx.Err()
	if repository.abandonmentContextErr != nil {
		return repository.abandonmentContextErr
	}
	repository.abandonments++
	repository.abandonedLocator = claim.Locator
	return nil
}

func (repository *boundaryRepository) ClaimEnvironmentBlueprintStage(
	_ context.Context,
	request testblueprints.EnvironmentBlueprintStageClaimRequest,
) (testblueprints.EnvironmentBlueprintStageClaim, error) {
	repository.claims++
	if repository.retainClaim && repository.winner != nil {
		winner := *repository.winner
		winner.Intent.Ciphertext = append([]byte(nil), winner.Intent.Ciphertext...)
		winner.Existing = true
		return winner, nil
	}
	claim := testblueprints.EnvironmentBlueprintStageClaim{
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
	request testblueprints.EnvironmentBlueprintStageRequest,
) (testblueprints.EnvironmentBlueprintSeal, error) {
	repository.stages++
	repository.stagedTaskID = request.Claim.TaskID
	projection, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(request.Projection)
	if err != nil {
		return testblueprints.EnvironmentBlueprintSeal{}, err
	}
	return testblueprints.EnvironmentBlueprintSeal{
		EnvironmentID:        request.Claim.EnvironmentID,
		RevisionID:           request.Claim.RevisionID,
		SourceKind:           request.Claim.SourceKind,
		RenderGeneration:     request.Claim.RenderGeneration,
		ProjectionSchema:     request.Claim.ProjectionSchema,
		BaselineHeadRevision: request.Claim.BaselineHeadRevision,
		DependencyDigest:     request.DependencyDigest,
		ProjectionBytes:      uint64(len(projection)),
		ProjectionSHA256:     sha256.Sum256(projection),
	}, nil
}

func (repository *boundaryRepository) PublishEnvironmentBlueprintDesiredRevision(
	_ context.Context,
	_ netip.Prefix,
	_ string,
	_ testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	_ testkeyvalue.Versioned[testhierarchy.EnvironmentRecord],
	_ int64,
	claim testblueprints.EnvironmentBlueprintStageClaim,
	_ testblueprints.EnvironmentDesiredRevisionIdentity,
	_ testenvironmentprojection.EnvironmentComposeProjection,
	_ []testblueprints.EnvironmentBlueprintZoneChange,
	_ []testblueprints.EnvironmentBlueprintServiceChange,
	_ []testblueprints.EnvironmentBlueprintRouteChange,
	_ testreleasegroups.ReleaseGroupBlueprintPreparedMutation,
	_ testcomponentplanning.ComponentTaskPreparation,
	_ testblueprintplanning.BlueprintAttachTaskPreparation,
	_ testblueprintplanning.BlueprintBackupPolicyPreparation,
	_ etcd.BlueprintScriptPublication,
	_ etcd.BlueprintReleasePublication,
	_ etcd.BlueprintRequirementGate,
	task etcd.TaskRecord,
	_ testidempotency.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.publications++
	repository.publishedClaimTaskID = claim.TaskID
	repository.publishedTaskID = task.ID
	if repository.publicationStarted != nil {
		close(repository.publicationStarted)
		<-repository.publicationRelease
	}
	return etcd.IdempotencyTransactionResult{}, repository.publicationErr
}

type boundaryPublicationIdempotency struct {
	known        idempotentintent.Resolution
	knownErr     error
	unknown      idempotentintent.Resolution
	unknownErr   error
	knownCalls   int
	unknownCalls int
}

func (idempotency *boundaryPublicationIdempotency) ResolveKnown(
	_ context.Context,
	_ Evidence,
	_ etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	idempotency.knownCalls++
	return idempotency.known, idempotency.knownErr
}

func (idempotency *boundaryPublicationIdempotency) ResolveUnknown(
	_ context.Context,
	_ testidempotency.IdempotencyLocator,
	_ Evidence,
	_ error,
) (idempotentintent.Resolution, error) {
	idempotency.unknownCalls++
	return idempotency.unknown, idempotency.unknownErr
}

func TestPreflightAndClaimEnforcesExactNormalizedProjectionBoundary(t *testing.T) {
	t.Parallel()
	const limit = uint64(2 * 1024 * 1024)
	now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	revisionID := ids.NewAt(ids.KindTask, now, 2)

	low, high := 1, int(limit)
	var exact testenvironmentprojection.EnvironmentComposeProjection
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
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(exact.ComposeArtifact, artifact); err != nil {
		t.Fatalf("unmarshal boundary projection artifact: %v", err)
	}
	baseYAMLBytes := len(artifact.CanonicalYaml)
	foundExact := false
	var exactErr error
	var bestBytes uint64
	bestDelta, bestPath, bestGeneration := 0, 0, uint64(1)
	for generation := uint64(1); generation <= 128 && !foundExact; generation *= 128 {
		for yamlDelta := 0; yamlDelta <= 32 && !foundExact; yamlDelta++ {
			if yamlDelta > baseYAMLBytes {
				break
			}
			low, high := 0, int(limit)
			for low <= high {
				middle := low + (high-low)/2
				candidate := boundaryProjection(
					t, now, environmentID, revisionID, baseYAMLBytes-yamlDelta, middle,
				)
				candidate.DesiredRoutes[0].DesiredGeneration = generation
				candidateEvidence, candidateErr := PreflightProjection(candidate)
				exactErr = candidateErr
				if candidateErr != nil {
					high = middle - 1
					continue
				}
				if candidateEvidence.NormalizedBytes > bestBytes {
					bestBytes, bestDelta, bestPath, bestGeneration = candidateEvidence.NormalizedBytes, yamlDelta, middle, generation
				}
				if candidateEvidence.NormalizedBytes == limit {
					exact, exactEvidence, foundExact = candidate, candidateEvidence, true
					break
				}
				low = middle + 1
			}
		}
	}
	if !foundExact || exactEvidence.NormalizedBytes != limit {
		t.Fatalf("exact normalized projection = %d, %v (best=%d delta=%d path=%d generation=%d)",
			exactEvidence.NormalizedBytes, exactErr, bestBytes, bestDelta, bestPath, bestGeneration)
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
	over.DesiredRoutes = append([]testenvironmentprojection.EnvironmentRouteProjection(nil), exact.DesiredRoutes...)
	over.DesiredRoutes[0].Desired.Path += "a"
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

	staged, err := Stage(ctx, repository, StageInput{
		Claim: recovered, Projection: recoveredProjection,
	})
	if err != nil {
		t.Fatalf("Stage(recovered) error = %v", err)
	}
	if staged.TaskID() != recovered.TaskID ||
		staged.state.seal.DependencyDigest != recoveredProjectionEvidence.DependencyDigest {
		t.Fatalf("staged authority = %q/%x", staged.TaskID(), staged.state.seal.DependencyDigest)
	}
	if _, err := repository.PublishEnvironmentBlueprintDesiredRevision(
		ctx,
		netip.Prefix{},
		"", testkeyvalue.Versioned[testhierarchy.ProjectRecord]{}, testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{}, 0,
		staged.state.claim, testblueprints.EnvironmentDesiredRevisionIdentity{EnvironmentID: environmentID, RevisionID: recovered.TaskID}, staged.state.projection,
		nil,
		nil,
		nil, testreleasegroups.ReleaseGroupBlueprintPreparedMutation{}, testcomponentplanning.ComponentTaskPreparation{}, testblueprintplanning.BlueprintAttachTaskPreparation{}, testblueprintplanning.BlueprintBackupPolicyPreparation{}, etcd.BlueprintScriptPublication{},
		etcd.BlueprintReleasePublication{},
		etcd.BlueprintRequirementGate{},
		etcd.TaskRecord{ID: recovered.TaskID}, testidempotency.IdempotencyMarker{},
	); err != nil {
		t.Fatalf("PublishEnvironmentBlueprintDesiredRevision(recovered) error = %v", err)
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

func TestPublishRejectsLocatorMismatchAndAbandonsExactStagedClaim(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 2, 8, 0, 0, 0, time.UTC)
	repository, claim, input := boundaryStagedPublication(t, now, 31)
	input.Locator.Key += "-mismatch"
	idempotency := &boundaryPublicationIdempotency{}

	if _, err := Publish(context.Background(), repository, idempotency, input); err == nil {
		t.Fatal("Publish(locator mismatch) error = nil")
	}
	if repository.publications != 0 || repository.abandonments != 1 ||
		repository.abandonedLocator != claim.Locator || idempotency.knownCalls != 0 || idempotency.unknownCalls != 0 {
		t.Fatalf(
			"locator mismatch = publications:%d abandonments:%d locator:%#v resolutions:%d/%d",
			repository.publications, repository.abandonments, repository.abandonedLocator,
			idempotency.knownCalls, idempotency.unknownCalls,
		)
	}
	input.Locator = claim.Locator
	if _, err := Publish(context.Background(), repository, idempotency, input); !errors.Is(
		err, errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("Publish(consumed locator) error = %v", err)
	}
	if repository.publications != 0 || repository.abandonments != 1 {
		t.Fatalf("repeated locator publication = %d/%d", repository.publications, repository.abandonments)
	}
}

func TestPublishConsumesCopiedStagedTokenExactlyOnceConcurrently(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 2, 8, 15, 0, 0, time.UTC)
	repository, _, input := boundaryStagedPublication(t, now, 32)
	repository.publicationStarted = make(chan struct{})
	repository.publicationRelease = make(chan struct{})
	idempotency := &boundaryPublicationIdempotency{known: idempotentintent.Resolution{
		Kind: idempotentintent.ResolutionApplied,
	}}
	copied := input
	firstResult := make(chan error, 1)
	go func() {
		_, err := Publish(context.Background(), repository, idempotency, input)
		firstResult <- err
	}()
	<-repository.publicationStarted
	if _, err := Publish(context.Background(), repository, idempotency, copied); !errors.Is(
		err, errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("Publish(concurrent copy) error = %v", err)
	}
	close(repository.publicationRelease)
	if err := <-firstResult; err != nil {
		t.Fatalf("Publish(first) error = %v", err)
	}
	if repository.publications != 1 || repository.abandonments != 0 || idempotency.knownCalls != 1 {
		t.Fatalf(
			"copied publication = publications:%d abandonments:%d resolutions:%d",
			repository.publications, repository.abandonments, idempotency.knownCalls,
		)
	}
}

func TestPublishKnownConflictAbandonsButUnresolvedUnknownCannotRetry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 2, 8, 30, 0, 0, time.UTC)
	knownRepository, knownClaim, knownInput := boundaryStagedPublication(t, now, 33)
	knownRepository.publicationErr = errs.New(errs.KindStateConflict, "known head conflict")
	if _, err := Publish(
		context.Background(), knownRepository, &boundaryPublicationIdempotency{}, knownInput,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Publish(known conflict) error = %v", err)
	}
	if knownRepository.publications != 1 || knownRepository.abandonments != 1 ||
		knownRepository.abandonedLocator != knownClaim.Locator {
		t.Fatalf(
			"known conflict = publications:%d abandonments:%d locator:%#v",
			knownRepository.publications, knownRepository.abandonments, knownRepository.abandonedLocator,
		)
	}

	unknownRepository, _, unknownInput := boundaryStagedPublication(t, now.Add(time.Minute), 34)
	unknownRepository.publicationErr = context.DeadlineExceeded
	unresolved := errors.New("publication outcome remains unknown")
	unknownIdempotency := &boundaryPublicationIdempotency{unknownErr: unresolved}
	if _, err := Publish(context.Background(), unknownRepository, unknownIdempotency, unknownInput); !errors.Is(
		err,
		unresolved,
	) {
		t.Fatalf("Publish(unresolved unknown) error = %v", err)
	}
	if _, err := Publish(context.Background(), unknownRepository, unknownIdempotency, unknownInput); !errors.Is(
		err, errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("Publish(unresolved retry) error = %v", err)
	}
	if unknownRepository.publications != 1 || unknownRepository.abandonments != 0 ||
		unknownIdempotency.unknownCalls != 1 {
		t.Fatalf(
			"unknown outcome = publications:%d abandonments:%d resolutions:%d",
			unknownRepository.publications, unknownRepository.abandonments, unknownIdempotency.unknownCalls,
		)
	}
}

// Rationale: request cancellation must not prevent the mandatory abandonment of a known failed staged publication.
func TestAbandonUsesBoundedContextAfterRequestCancellation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 2, 8, 45, 0, 0, time.UTC)
	repository, claim, input := boundaryStagedPublication(t, now, 35)
	cause := errs.New(errs.KindStateConflict, "known pre-publication failure")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := Abandon(ctx, repository, input.Staged, cause); !errors.Is(err, cause) {
		t.Fatalf("Abandon(canceled request) error = %v", err)
	}
	if repository.abandonments != 1 || repository.abandonedLocator != claim.Locator ||
		repository.abandonmentContextErr != nil {
		t.Fatalf(
			"canceled abandonment = count:%d locator:%#v context:%v",
			repository.abandonments,
			repository.abandonedLocator,
			repository.abandonmentContextErr,
		)
	}
}

func boundaryStagedPublication(
	t *testing.T,
	now time.Time,
	sequence int64,
) (*boundaryRepository, testblueprints.EnvironmentBlueprintStageClaim, PublishInput) {
	t.Helper()
	ctx := context.Background()
	environmentID := ids.NewAt(ids.KindEnvironment, now, sequence)
	taskID := ids.NewAt(ids.KindTask, now, sequence+1)
	repository := &boundaryRepository{}
	claim, err := Claim(ctx, repository, boundaryClaimInput(now, environmentID, taskID))
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	projection := boundaryProjection(t, now, environmentID, taskID, 64, 0)
	staged, err := Stage(ctx, repository, StageInput{Claim: claim, Projection: projection})
	if err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	return repository, claim, PublishInput{
		Environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{ID: environmentID},
		},
		Staged:  staged,
		Locator: claim.Locator,
		Task:    etcd.TaskRecord{ID: taskID},
	}
}

func boundaryProjection(
	t *testing.T,
	now time.Time,
	environmentID string,
	revisionID string,
	yamlBytes int,
	pathPadding int,
) testenvironmentprojection.EnvironmentComposeProjection {
	t.Helper()
	canonicalYAML := []byte(strings.Repeat("x", yamlBytes))
	serviceID := ids.NewAt(ids.KindService, now, 6)
	networkID := ids.NewAt(ids.KindNetwork, now, 5)
	routeID := ids.NewAt(ids.KindRoute, now, 4)
	digest := sha256.Sum256(canonicalYAML)
	artifact, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: ids.NewAt(ids.KindConfig, now, 3),
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:    environmentID, ProjectName: "boundary",
		CanonicalYaml: canonicalYAML, YamlSha256: digest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/boundary",
		Services:            []*agentpb.ComposeService{{ServiceId: serviceID, ComposeName: "api"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 1,
		ComposeArtifact: artifact, NormalizedCompose: []byte("services: {}\n"),
		DesiredZones: []testenvironmentprojection.EnvironmentZoneProjection{
			{EnvironmentID: environmentID, Desired: core.Zone{
				ID: networkID, Name: "frontend", Subnet: "10.70.0.0/24",
				OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
			}},
		},
		DesiredServices: []testservices.EnvironmentServiceProjection{
			{EnvironmentID: environmentID, Desired: core.Service{
				ID: serviceID, Name: "api", Image: "example.invalid/api:1",
				Zones: []string{"frontend"}, Strategy: core.StrategyRecreate, Replicas: 1,
			}},
		},
		DesiredRoutes: []testenvironmentprojection.EnvironmentRouteProjection{{
			EnvironmentID: environmentID, DesiredGeneration: 1, Desired: core.Route{
				ID: routeID, Host: "boundary.example.test",
				Path: "/" + strings.Repeat("a", pathPadding), TargetServiceID: serviceID,
				TargetPort: 8080, Exposure: "public",
			},
		}},
	}
}

func boundaryClaimInput(now time.Time, environmentID, revisionID string) ClaimInput {
	ciphertext := []byte("protected-boundary-intent")
	digest := sha256.Sum256(ciphertext)
	intent := testidempotency.ProtectedIntentRecord{
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextDigest: stringDigest(digest), Ciphertext: ciphertext,
	}
	return ClaimInput{
		EnvironmentID: environmentID, CandidateTaskID: revisionID,
		Locator: testidempotency.IdempotencyLocator{
			ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environmentID,
			Method: "PUT", Route: blueprintRoute, Key: "boundary-idempotency-key-0001",
		},
		Intent: intent,
		MatchExistingIntent: func(_ context.Context, existing testidempotency.ProtectedIntentRecord) (bool, error) {
			return reflect.DeepEqual(existing, intent), nil
		},
		SourceKind: testblueprints.EnvironmentBlueprintSourceApply, RenderGeneration: 1, CreatedAt: now,
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
