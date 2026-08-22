package app

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type fakeEntryReadRepository struct {
	entryReadRepository
	environment     etcd.Versioned[etcd.EnvironmentRecord]
	entry           etcd.Versioned[etcd.EntryRecord]
	secretValue     etcd.SecretEntryValueGeneration
	page            etcd.Page[etcd.EntryRecord]
	wantPage        etcd.PageRequest
	environmentRead bool
	listed          bool
}

func (fake *fakeEntryReadRepository) GetEnvironment(
	_ context.Context,
	id string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	fake.environmentRead = id == fake.environment.Record.ID
	return fake.environment, nil
}

func (fake *fakeEntryReadRepository) GetEntry(
	_ context.Context,
	id string,
) (etcd.Versioned[etcd.EntryRecord], error) {
	if id != fake.entry.Record.Entry.ID {
		return etcd.Versioned[etcd.EntryRecord]{}, errs.New(errs.KindEntryNotFound, "Entry was not found")
	}
	return fake.entry, nil
}

func (fake *fakeEntryReadRepository) ListEntries(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.EntryRecord], error) {
	fake.listed = environmentID == fake.environment.Record.ID && request == fake.wantPage
	return fake.page, nil
}

func (fake *fakeEntryReadRepository) GetSecretEntryValue(
	_ context.Context,
	entryID string,
	generationID string,
) (etcd.SecretEntryValueGeneration, bool, error) {
	if entryID != fake.entry.Record.Entry.ID || generationID != fake.entry.Record.CurrentValueGenerationID {
		return etcd.SecretEntryValueGeneration{}, false, errs.New(errs.KindInternal, "Entry generation changed")
	}
	return fake.secretValue, true, nil
}

// Rationale: Entry collection ownership is an Environment existence check, and cursor paging must
// remain the durable repository's fixed-revision page rather than an application-side reconstruction.
func TestEntryListVerifiesEnvironmentAndPreservesPagination(t *testing.T) {
	t.Parallel()
	environmentID := ids.NewAt(ids.KindEnvironment, secretReadTestTime(), 21)
	request := etcd.PageRequest{Limit: 37, Cursor: "opaque"}
	want := etcd.Page[etcd.EntryRecord]{NextCursor: "next", Revision: 88}
	repository := &fakeEntryReadRepository{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{
				NetworkPool: "10.40.0.0/16",
				ID:          environmentID,
			},
			Revision:     4,
			ReadRevision: 4,
		},
		page: want, wantPage: request,
	}
	service, err := newEntryReadService(repository, secretReadTestProtector(t))
	if err != nil {
		t.Fatalf("newEntryReadService() error = %v", err)
	}
	got, err := service.ListEntries(context.Background(), environmentID, request)
	if err != nil || !repository.environmentRead || !repository.listed ||
		got.NextCursor != want.NextCursor || got.Revision != want.Revision {
		t.Fatalf(
			"ListEntries() = %#v, %v, environment read %t, listed %t",
			got,
			err,
			repository.environmentRead,
			repository.listed,
		)
	}
}

// Rationale: reveal is the sole Entry plaintext response boundary, so it must open the exact selected
// encrypted generation, verify its ownership tuple, and clear repository ciphertext on return.
func TestEntryRevealDecryptsSelectedGenerationAndClearsCiphertext(t *testing.T) {
	t.Parallel()
	protector := secretReadTestProtector(t)
	environmentID := ids.NewAt(ids.KindEnvironment, secretReadTestTime(), 22)
	entryID := ids.NewAt(ids.KindEnvEntry, secretReadTestTime(), 23)
	generationID := ids.NewAt(ids.KindConfig, secretReadTestTime(), 24)
	envelope, err := protector.Seal(context.Background(), []byte("entry-password"))
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	metadata := envelope.Metadata()
	repository := &fakeEntryReadRepository{
		entry: etcd.Versioned[etcd.EntryRecord]{Record: etcd.EntryRecord{
			EnvironmentID: environmentID,
			Entry: core.EnvEntry{
				ID: entryID, Kind: core.EntryKindEnv, Key: "PASSWORD",
				Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"all"}, Secret: true,
			},
			CurrentValueGenerationID: generationID,
		}},
		secretValue: etcd.SecretEntryValueGeneration{
			EnvironmentID: environmentID, EntryID: entryID, GenerationID: generationID,
			EnvelopeVersion: uint8(metadata.Version), Cipher: string(metadata.Cipher),
			DigestAlgorithm: string(metadata.Digest.Algorithm), CiphertextSHA256: metadata.Digest.Value,
			Ciphertext: envelope.Ciphertext(),
		},
	}
	service, err := newEntryReadService(repository, protector)
	if err != nil {
		t.Fatalf("newEntryReadService() error = %v", err)
	}
	got, err := service.RevealEntry(context.Background(), entryID)
	if err != nil || got != "entry-password" {
		t.Fatalf("RevealEntry() = %q, %v", got, err)
	}
	if !allBytesZero(repository.secretValue.Ciphertext) {
		t.Fatal("RevealEntry() retained repository ciphertext")
	}
}

// Rationale: the value endpoint is a secret-only exception; allowing it for plain Entries would
// create a second read representation and blur the accepted desired-state storage boundary.
func TestEntryRevealRejectsPlainEntry(t *testing.T) {
	t.Parallel()
	entryID := ids.NewAt(ids.KindEnvEntry, secretReadTestTime(), 25)
	repository := &fakeEntryReadRepository{entry: etcd.Versioned[etcd.EntryRecord]{Record: etcd.EntryRecord{
		Entry: core.EnvEntry{ID: entryID},
	}}}
	service, err := newEntryReadService(repository, secretReadTestProtector(t))
	if err != nil {
		t.Fatalf("newEntryReadService() error = %v", err)
	}
	if value, revealErr := service.RevealEntry(context.Background(), entryID); value != "" ||
		!errors.Is(revealErr, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("RevealEntry() = %q, %v, want validation failure", value, revealErr)
	}
}
