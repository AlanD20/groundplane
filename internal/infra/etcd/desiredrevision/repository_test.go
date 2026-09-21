package desiredrevision

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestRepositoryClaimsWinnerAndSealsTypedMutationRevision(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	repository, err := newRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 26, 21, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	taskID := ids.NewAt(ids.KindTask, now, 2)
	ciphertext := []byte("protected-volume-add")
	intentDigest := sha256.Sum256(ciphertext)
	request := testblueprints.EnvironmentBlueprintStageClaimRequest{
		EnvironmentID: environmentID, CandidateRevisionID: taskID, CandidateTaskID: taskID,
		Locator: testidempotency.IdempotencyLocator{
			ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environmentID,
			Method: http.MethodPost, Route: "/environments/{id}/volumes", Key: "volume-add-key-0001",
		},
		Intent: testidempotency.ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(intentDigest[:]), Ciphertext: ciphertext,
		},
		SourceKind: testblueprints.EnvironmentBlueprintSourceMutation, RenderGeneration: 1,
		ProjectionSchema: testblueprints.EnvironmentDesiredProjectionSchema, CreatedAt: now,
	}
	claim, err := repository.ClaimEnvironmentBlueprintStage(ctx, request)
	if err != nil || claim.Existing {
		t.Fatalf("Claim() = %#v, %v", claim, err)
	}
	loser := request
	loser.CandidateRevisionID = ids.NewAt(ids.KindTask, now, 3)
	loser.CandidateTaskID = loser.CandidateRevisionID
	existing, err := repository.ClaimEnvironmentBlueprintStage(ctx, loser)
	if err != nil || !existing.Existing || existing.RevisionID != claim.RevisionID || existing.TaskID != claim.TaskID {
		t.Fatalf("Claim(replay) = %#v, %v", existing, err)
	}

	volumeID := ids.NewAt(ids.KindVolume, now, 4)
	projection := mutationProjection(t, now, environmentID, taskID, volumeID)
	dependencyDigest, err := testblueprints.EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		t.Fatal(err)
	}
	seal, err := repository.StageEnvironmentBlueprintRevision(ctx, testblueprints.EnvironmentBlueprintStageRequest{
		Claim: claim,
		Mutation: &testblueprints.EnvironmentDesiredMutationAudit{
			Volume: &testblueprints.EnvironmentVolumeMutationAudit{
				Action: testblueprints.EnvironmentVolumeMutationAdd, VolumeID: volumeID,
				Slug: "application-data", Key: "application_data", KeySupplied: true,
			},
		},
		Projection: projection, DependencyDigest: dependencyDigest,
	})
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != taskID ||
		seal.SourceKind != testblueprints.EnvironmentBlueprintSourceMutation || seal.ProjectionBytes == 0 {
		t.Fatalf("Stage() = %#v, %v", seal, err)
	}
	root, err := store.Get(ctx, testblueprints.EnvironmentBlueprintRootKey(environmentID, taskID))
	if err != nil || root.Entry == nil {
		t.Fatalf("sealed root = %#v, %v", root, err)
	}
	stored, err := testblueprints.DecodeEnvironmentBlueprintSeal(root.Entry.Value)
	if err != nil || stored != seal {
		t.Fatalf("sealed root decode = %#v, %v", stored, err)
	}
}

func mutationProjection(
	t *testing.T,
	now time.Time,
	environmentID, taskID, volumeID string,
) testenvironmentprojection.EnvironmentComposeProjection {
	t.Helper()
	canonicalYAML := []byte("services: {}\nvolumes:\n  application_data: {}\n")
	digest := sha256.Sum256(canonicalYAML)
	artifact, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: ids.NewAt(ids.KindConfig, now, 5),
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:    environmentID, ProjectName: "mutation", CanonicalYaml: canonicalYAML,
		YamlSha256: digest[:], AuthorizedVolumeDir: "/var/lib/groundplane/vol/mutation",
		Volumes: []*agentpb.ComposeVolume{{VolumeId: volumeID, ComposeName: "application_data"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID:     environmentID,
		RevisionID:        taskID,
		RenderGeneration:  1,
		ComposeArtifact:   artifact,
		NormalizedCompose: canonicalYAML,
		Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{
			{ID: volumeID, Slug: "application-data", Key: "application_data"},
		},
	}
}

type memoryVersion struct {
	revision int64
	value    []byte
	present  bool
}
type memoryStore struct {
	revision int64
	history  map[string][]memoryVersion
}

func newMemoryStore() *memoryStore { return &memoryStore{history: make(map[string][]memoryVersion)} }
func (store *memoryStore) Get(_ context.Context, key string) (*testkeyvalue.GetResult, error) {
	return &testkeyvalue.GetResult{Entry: store.valueAt(key, store.revision), ReadRevision: store.revision}, nil
}

func (store *memoryStore) GetMany(
	_ context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	revision := request.Revision
	if revision == 0 {
		revision = store.revision
	}
	values := make([]*testkeyvalue.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		values[index] = store.valueAt(key, revision)
	}
	return &testkeyvalue.GetManyResult{Values: values, ReadRevision: revision, ResponseRevision: store.revision}, nil
}

func (store *memoryStore) Range(
	_ context.Context,
	request testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	return &testkeyvalue.RangeResult{ReadRevision: store.revision, ResponseRevision: store.revision}, nil
}

func (store *memoryStore) MeasureTransaction(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionBudget, error) {
	return etcd.MeasureTransactionBudget(ctx, "/groundplane/", conditions, mutations)
}

func (store *memoryStore) Transact(
	_ context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	for _, condition := range conditions {
		value := store.valueAt(condition.Key, store.revision)
		actual := int64(0)
		if value != nil {
			actual = value.ModRevision
		}
		if actual != condition.ModRevision {
			failureReads := make([]*testkeyvalue.KeyValue, len(conditions))
			for index, failed := range conditions {
				failureReads[index] = store.valueAt(failed.Key, store.revision)
			}
			return testkeyvalue.TransactionResult{Revision: store.revision, FailureReads: failureReads}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		version := memoryVersion{revision: store.revision}
		switch mutation.Type {
		case testkeyvalue.MutationPut:
			version.present = true
			version.value = append([]byte(nil), mutation.Value...)
		case testkeyvalue.MutationDelete:
		default:
			panic("invalid mutation")
		}
		store.history[mutation.Key] = append(store.history[mutation.Key], version)
	}
	return testkeyvalue.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}
func (store *memoryStore) valueAt(key string, revision int64) *testkeyvalue.KeyValue {
	versions := store.history[key]
	for index := len(versions) - 1; index >= 0; index-- {
		version := versions[index]
		if version.revision > revision {
			continue
		}
		if !version.present {
			return nil
		}
		count := int64(0)
		for previous := index; previous >= 0 && versions[previous].present; previous-- {
			count++
		}
		return &testkeyvalue.KeyValue{
			Key:         key,
			Value:       append([]byte(nil), version.value...),
			Version:     count,
			ModRevision: version.revision,
		}
	}
	return nil
}
