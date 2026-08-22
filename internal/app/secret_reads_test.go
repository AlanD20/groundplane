package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type fakeSecretReadRepository struct {
	secretReadRepository
	project        etcd.Versioned[etcd.ProjectRecord]
	secret         etcd.Versioned[etcd.SecretRecord]
	value          etcd.SecretEncryptedValue
	page           etcd.Page[etcd.SecretRecord]
	wantRequest    etcd.PageRequest
	wantScope      core.SecretScope
	wantProjectID  string
	projectChecked bool
	listed         bool
}

func (fake *fakeSecretReadRepository) GetProject(
	_ context.Context,
	id string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	fake.projectChecked = id == fake.project.Record.ID
	return fake.project, nil
}

func (fake *fakeSecretReadRepository) GetSecret(
	_ context.Context,
	id string,
) (etcd.Versioned[etcd.SecretRecord], error) {
	if id != fake.secret.Record.Secret.ID {
		return etcd.Versioned[etcd.SecretRecord]{}, errs.New(errs.KindSecretNotFound, "Secret was not found")
	}
	return fake.secret, nil
}

func (fake *fakeSecretReadRepository) GetSecretValue(
	_ context.Context,
	current etcd.Versioned[etcd.SecretRecord],
) (etcd.SecretEncryptedValue, error) {
	if current.Record.Secret.ID != fake.secret.Record.Secret.ID {
		return etcd.SecretEncryptedValue{}, errs.New(errs.KindInternal, "Secret version changed")
	}
	return fake.value, nil
}

func (fake *fakeSecretReadRepository) ListSecrets(
	_ context.Context,
	scope core.SecretScope,
	projectID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.SecretRecord], error) {
	fake.listed = scope == fake.wantScope && projectID == fake.wantProjectID && request == fake.wantRequest
	return fake.page, nil
}

type identitySecretReadCipher struct{}

func (identitySecretReadCipher) Seal(_ context.Context, plaintext []byte) ([]byte, error) {
	return append([]byte(nil), plaintext...), nil
}

func (identitySecretReadCipher) Open(_ context.Context, ciphertext []byte) ([]byte, error) {
	return append([]byte(nil), ciphertext...), nil
}

// Rationale: project-scoped labels must verify their durable owner and preserve the repository's
// fixed-revision cursor page, while platform reads must not invent a hierarchy owner.
func TestSecretListVerifiesOnlyProjectOwnerAndPreservesPagination(t *testing.T) {
	t.Parallel()
	projectID := ids.NewAt(ids.KindProject, secretReadTestTime(), 1)
	request := etcd.PageRequest{Limit: 23, Cursor: "opaque-cursor"}
	want := etcd.Page[etcd.SecretRecord]{NextCursor: "next-cursor", Revision: 91}
	repository := &fakeSecretReadRepository{
		project: etcd.Versioned[etcd.ProjectRecord]{
			Record: etcd.ProjectRecord{ID: projectID}, Revision: 12, ReadRevision: 12,
		},
		page: want, wantRequest: request, wantScope: core.SecretScopeProject, wantProjectID: projectID,
	}
	service, err := newSecretReadService(repository, secretReadTestProtector(t))
	if err != nil {
		t.Fatalf("newSecretReadService() error = %v", err)
	}
	got, err := service.ListSecrets(context.Background(), core.SecretScopeProject, projectID, request)
	if err != nil || !repository.projectChecked || !repository.listed || got.NextCursor != want.NextCursor ||
		got.Revision != want.Revision {
		t.Fatalf(
			"ListSecrets() = %#v, %v, project checked %t, listed %t",
			got,
			err,
			repository.projectChecked,
			repository.listed,
		)
	}
}

// Rationale: reveal is the sole human plaintext boundary, so it must decrypt the exact ciphertext
// paired with the metadata revision and clear the repository-owned ciphertext buffer afterward.
func TestSecretRevealDecryptsExactValueAndClearsCiphertext(t *testing.T) {
	t.Parallel()
	protector := secretReadTestProtector(t)
	secretID := ids.NewAt(ids.KindSecret, secretReadTestTime(), 2)
	envelope, err := protector.Seal(context.Background(), []byte("database-password"))
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	metadata := envelope.Metadata()
	repository := &fakeSecretReadRepository{
		secret: etcd.Versioned[etcd.SecretRecord]{
			Record: etcd.SecretRecord{Secret: core.Secret{ID: secretID}}, Revision: 31, ReadRevision: 31,
		},
		value: etcd.SecretEncryptedValue{
			SecretID: secretID, EnvelopeVersion: uint8(metadata.Version), Cipher: string(metadata.Cipher),
			DigestAlgorithm: string(metadata.Digest.Algorithm), CiphertextSHA256: metadata.Digest.Value,
			Ciphertext: envelope.Ciphertext(),
		},
	}
	service, err := newSecretReadService(repository, protector)
	if err != nil {
		t.Fatalf("newSecretReadService() error = %v", err)
	}
	got, err := service.RevealSecret(context.Background(), secretID)
	if err != nil || got != "database-password" {
		t.Fatalf("RevealSecret() = %q, %v", got, err)
	}
	if !allBytesZero(repository.value.Ciphertext) {
		t.Fatal("RevealSecret() retained repository ciphertext")
	}
}

// Rationale: the public reveal DTO is a JSON string, so malformed durable plaintext must fail
// closed instead of being silently replaced by the JSON encoder.
func TestSecretRevealRejectsNonUTF8Plaintext(t *testing.T) {
	t.Parallel()
	protector := secretReadTestProtector(t)
	secretID := ids.NewAt(ids.KindSecret, secretReadTestTime(), 3)
	envelope, err := protector.Seal(context.Background(), []byte{0xff, 0xfe})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	metadata := envelope.Metadata()
	repository := &fakeSecretReadRepository{
		secret: etcd.Versioned[etcd.SecretRecord]{
			Record: etcd.SecretRecord{Secret: core.Secret{ID: secretID}}, Revision: 32, ReadRevision: 32,
		},
		value: etcd.SecretEncryptedValue{
			SecretID: secretID, EnvelopeVersion: uint8(metadata.Version), Cipher: string(metadata.Cipher),
			DigestAlgorithm: string(metadata.Digest.Algorithm), CiphertextSHA256: metadata.Digest.Value,
			Ciphertext: envelope.Ciphertext(),
		},
	}
	service, err := newSecretReadService(repository, protector)
	if err != nil {
		t.Fatalf("newSecretReadService() error = %v", err)
	}
	if value, revealErr := service.RevealSecret(context.Background(), secretID); value != "" ||
		!errors.Is(revealErr, errs.New(errs.KindInternal, "")) {
		t.Fatalf("RevealSecret() = %q, %v, want internal failure", value, revealErr)
	}
}

func secretReadTestProtector(t *testing.T) *secretvalue.Protector {
	t.Helper()
	cipher := identitySecretReadCipher{}
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatalf("NewProtector() error = %v", err)
	}
	return protector
}

func secretReadTestTime() time.Time {
	return time.Date(2026, time.August, 22, 14, 0, 0, 0, time.UTC)
}
