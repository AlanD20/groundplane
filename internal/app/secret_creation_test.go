package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const secretCreationTestProjectID = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"

// Rationale: protected creation must seal the write-only value, atomically
// persist its exact replay response, and return only redacted metadata.
func TestSecretCreationSealsValueAndReturnsRedactedMetadata(t *testing.T) {
	t.Parallel()
	repository := &fakeSecretCreationRepository{project: etcd.Versioned[etcd.ProjectRecord]{
		Record:   etcd.ProjectRecord{ID: secretCreationTestProjectID, Kind: etcd.ProjectKindTenant},
		Revision: 7, ReadRevision: 7,
	}}
	idempotency := &fakeSecretCreationIdempotency{
		evidence:   secretCreationTestEvidence(),
		resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
	}
	protector, err := secretvalue.NewProtector(identitySecretCreationCipher{}, identitySecretCreationCipher{})
	if err != nil {
		t.Fatalf("NewProtector() error = %v", err)
	}
	service, err := newSecretCreationService(repository, protector, idempotency)
	if err != nil {
		t.Fatalf("newSecretCreationService() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 19, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	input := apiTypes.SecretCreateRequest{
		ProjectID: secretCreationTestProjectID, Key: "API_TOKEN", Kind: "env_var", Value: "private-token",
	}
	response, err := service.CreateSecret(context.Background(), input, "secret-create-key-0002")
	if err != nil {
		t.Fatalf("CreateSecret() error = %v", err)
	}
	var public apiTypes.Secret
	if err := json.Unmarshal(response.Body, &public); err != nil {
		t.Fatalf("decode redacted response: %v", err)
	}
	if response.Status != http.StatusCreated || public.ID == "" || public.ProjectID != secretCreationTestProjectID ||
		public.Key != "API_TOKEN" || public.Kind != "env_var" || public.Scope != "project" ||
		bytes.Contains(response.Body, []byte("private-token")) {
		t.Fatalf("CreateSecret() returned invalid redacted metadata: %#v", public)
	}
	if repository.owner.Project == nil || repository.owner.Project.Record.ID != secretCreationTestProjectID ||
		repository.record.Secret.ID != public.ID || string(repository.value.Ciphertext) != "private-token" ||
		!reflect.DeepEqual(repository.marker.Response, response) {
		t.Fatalf("persisted Secret metadata does not match the returned resource")
	}
}

// Rationale: an exact replay must return before owner lookup or encryption so
// retries do not depend on a subsequently deleted Project or re-read plaintext.
func TestSecretCreationReplaysBeforeRepositoryAccess(t *testing.T) {
	t.Parallel()
	want := etcd.IdempotencyResponse{
		Status:      http.StatusCreated,
		ContentKind: "application/json",
		Body: []byte(
			`{"id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAV","scope":"platform","key":"TOKEN","kind":"env_var","ref":"secrets/.env.edge","updated_at":"2026-08-22T19:00:00Z"}`,
		),
	}
	repository := &fakeSecretCreationRepository{}
	protector, err := secretvalue.NewProtector(identitySecretCreationCipher{}, identitySecretCreationCipher{})
	if err != nil {
		t.Fatalf("NewProtector() error = %v", err)
	}
	service, err := newSecretCreationService(repository, protector, &fakeSecretCreationIdempotency{
		evidence: secretCreationTestEvidence(), existing: true,
		resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionReplay, Response: want},
	})
	if err != nil {
		t.Fatalf("newSecretCreationService() error = %v", err)
	}
	got, err := service.CreateSecret(context.Background(), apiTypes.SecretCreateRequest{
		Platform: true, Key: "TOKEN", Kind: "env_var", Value: "same-value",
	}, "secret-create-key-0003")
	if err != nil || !reflect.DeepEqual(got, want) || repository.calls != 0 {
		t.Fatalf("CreateSecret(replay) response/error/repository calls = %#v/%v/%d", got, err, repository.calls)
	}
}

type fakeSecretCreationRepository struct {
	project etcd.Versioned[etcd.ProjectRecord]
	owner   etcd.SecretOwner
	record  etcd.SecretRecord
	value   etcd.SecretEncryptedValue
	marker  etcd.IdempotencyMarker
	calls   int
}

func (repository *fakeSecretCreationRepository) GetProject(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	repository.calls++
	return repository.project, nil
}

func (repository *fakeSecretCreationRepository) CreateSecretIdempotent(
	_ context.Context,
	owner etcd.SecretOwner,
	record etcd.SecretRecord,
	value etcd.SecretEncryptedValue,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.calls++
	repository.owner = owner
	repository.record = record
	repository.value = value
	repository.value.Ciphertext = append([]byte(nil), value.Ciphertext...)
	repository.marker = marker
	repository.marker.Intent.Ciphertext = append([]byte(nil), marker.Intent.Ciphertext...)
	repository.marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeSecretCreationIdempotency struct {
	evidence   secretCreationEvidence
	resolution idempotentintent.Resolution
	existing   bool
}

func (idempotency *fakeSecretCreationIdempotency) Prepare(
	_ context.Context,
	_ apiTypes.SecretCreateRequest,
) (secretCreationEvidence, error) {
	return idempotency.evidence, nil
}

func (idempotency *fakeSecretCreationIdempotency) ResolveExisting(
	_ context.Context,
	_ etcd.IdempotencyLocator,
	_ secretCreationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.resolution, idempotency.existing, nil
}

func (idempotency *fakeSecretCreationIdempotency) ResolveKnown(
	_ context.Context,
	_ secretCreationEvidence,
	_ etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func (idempotency *fakeSecretCreationIdempotency) ResolveUnknown(
	_ context.Context,
	_ etcd.IdempotencyLocator,
	_ secretCreationEvidence,
	_ error,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

type identitySecretCreationCipher struct{}

func (identitySecretCreationCipher) Seal(_ context.Context, value []byte) ([]byte, error) {
	return append([]byte(nil), value...), nil
}

func (identitySecretCreationCipher) Open(_ context.Context, value []byte) ([]byte, error) {
	return append([]byte(nil), value...), nil
}

func secretCreationTestEvidence() secretCreationEvidence {
	ciphertext := []byte("protected-intent")
	digest := sha256.Sum256(ciphertext)
	return secretCreationEvidence{durable: etcd.ProtectedIntentRecord{
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
	}}
}
