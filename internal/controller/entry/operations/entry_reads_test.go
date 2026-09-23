package operations

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testentryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type fakeEntryReadRepository struct {
	entryReadRepository
	environment     testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	entry           testkeyvalue.Versioned[testentries.Record]
	secretValue     testentryvalues.SecretGeneration
	page            testkeyvalue.Page[testentries.Record]
	wantPage        testkeyvalue.PageRequest
	environmentRead bool
	listed          bool
	projection      *testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]
}

func (fake *fakeEntryReadRepository) GetEnvironment(
	_ context.Context,
	id string,
) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	fake.environmentRead = id == fake.environment.Record.ID
	return fake.environment, nil
}

func (fake *fakeEntryReadRepository) GetEntry(
	_ context.Context,
	id string,
) (testkeyvalue.Versioned[testentries.Record], error) {
	if id != fake.entry.Record.Entry.ID {
		return testkeyvalue.Versioned[testentries.Record]{}, errs.New(errs.KindEntryNotFound, "Entry was not found")
	}
	return fake.entry, nil
}

func (fake *fakeEntryReadRepository) ListEntries(
	_ context.Context,
	environmentID string,
	request testkeyvalue.PageRequest,
) (testkeyvalue.Page[testentries.Record], error) {
	fake.listed = environmentID == fake.environment.Record.ID && request == fake.wantPage
	return fake.page, nil
}

func (fake *fakeEntryReadRepository) GetEnvironmentComposeProjection(
	context.Context,
	string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	if fake.projection != nil {
		return *fake.projection, true, nil
	}
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{}, false, nil
}

func (fake *fakeEntryReadRepository) GetEnvironmentComposeProjectionRevision(
	context.Context,
	string,
	string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	if fake.projection != nil {
		return *fake.projection, true, nil
	}
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{}, false, nil
}

func TestEntryListPagesCurrentProjection(t *testing.T) {
	t.Parallel()
	now := secretReadTestTime()
	environmentID := ids.NewAt(ids.KindEnvironment, now, 31)
	revisionID := ids.NewAt(ids.KindTask, now, 32)
	records := make([]testentries.Record, 2)
	for index := range records {
		var err error
		records[index], err = testentries.NewBlueprintRecord(environmentID, string(rune('a'+index)), core.EnvEntry{
			ID: ids.NewAt(ids.KindEnvEntry, now, int64(33+index)), Kind: core.EntryKindEnv,
			Key: string(
				rune('A' + index),
			), Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"all"},
		}, ids.NewAt(ids.KindConfig, now, int64(35+index)))
		if err != nil {
			t.Fatal(err)
		}
	}
	projection := testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record: testenvironmentprojection.EnvironmentComposeProjection{
			EnvironmentID: environmentID,
			RevisionID:    revisionID,
			Entries:       records,
		},
		Revision: 12, ReadRevision: 12,
	}
	repository := &fakeEntryReadRepository{
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{ID: environmentID},
		},
		projection: &projection,
	}
	service, err := NewReadService(repository, secretReadTestProtector(t))
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.ListEntries(context.Background(), environmentID, testkeyvalue.PageRequest{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.NextCursor == "" {
		t.Fatalf("first projection page = %#v, %v", first, err)
	}
	second, err := service.ListEntries(
		context.Background(),
		environmentID, testkeyvalue.PageRequest{Limit: 1, Cursor: first.NextCursor},
	)
	if err != nil || len(second.Items) != 1 || second.NextCursor != "" {
		t.Fatalf("second projection page = %#v, %v", second, err)
	}
}

func (fake *fakeEntryReadRepository) GetSecretEntryValue(
	_ context.Context,
	entryID string,
	generationID string,
) (testentryvalues.SecretGeneration, bool, error) {
	if entryID != fake.entry.Record.Entry.ID || generationID != fake.entry.Record.CurrentValueGenerationID {
		return testentryvalues.SecretGeneration{}, false, errs.New(errs.KindInternal, "Entry generation changed")
	}
	return fake.secretValue, true, nil
}

// Rationale: Entry collection ownership is an Environment existence check, and cursor paging must
// remain the durable repository's fixed-revision page rather than an application-side reconstruction.
func TestEntryListVerifiesEnvironmentAndPreservesPagination(t *testing.T) {
	t.Parallel()
	environmentID := ids.NewAt(ids.KindEnvironment, secretReadTestTime(), 21)
	request := testkeyvalue.PageRequest{Limit: 37, Cursor: "opaque"}
	want := testkeyvalue.Page[testentries.Record]{NextCursor: "next", Revision: 88}
	repository := &fakeEntryReadRepository{
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{
				NetworkPool: "10.40.0.0/16",
				ID:          environmentID,
			},
			Revision:     4,
			ReadRevision: 4,
		},
		page: want, wantPage: request,
	}
	service, err := NewReadService(repository, secretReadTestProtector(t))
	if err != nil {
		t.Fatalf("NewReadService() error = %v", err)
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
		entry: testkeyvalue.Versioned[testentries.Record]{Record: testentries.Record{
			EnvironmentID: environmentID,
			Entry: core.EnvEntry{
				ID: entryID, Kind: core.EntryKindEnv, Key: "PASSWORD",
				Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"all"}, Secret: true,
			},
			CurrentValueGenerationID: generationID,
		}},
		secretValue: testentryvalues.SecretGeneration{
			EnvironmentID: environmentID, EntryID: entryID, GenerationID: generationID,
			EnvelopeVersion: uint8(metadata.Version), Cipher: string(metadata.Cipher),
			DigestAlgorithm: string(metadata.Digest.Algorithm), CiphertextSHA256: metadata.Digest.Value,
			Ciphertext: envelope.Ciphertext(),
		},
	}
	service, err := NewReadService(repository, protector)
	if err != nil {
		t.Fatalf("NewReadService() error = %v", err)
	}
	got, err := service.RevealEntry(context.Background(), entryID)
	if err != nil || got != "entry-password" {
		t.Fatalf("RevealEntry() = %q, %v", got, err)
	}
	if !allBytesZero(repository.secretValue.Ciphertext) {
		t.Fatal("RevealEntry() retained repository ciphertext")
	}
}

// Rationale: redacted secret metadata is identical for empty and non-empty values;
// the list badge must inspect the exact selected generation without returning its bytes.
func TestEntryEmptySecretStatusUsesSelectedGeneration(t *testing.T) {
	t.Parallel()
	protector := secretReadTestProtector(t)
	for index, value := range []string{"", "entry-password"} {
		entryID := ids.NewAt(ids.KindEnvEntry, secretReadTestTime(), int64(41+index))
		generationID := ids.NewAt(ids.KindConfig, secretReadTestTime(), int64(43+index))
		environmentID := ids.NewAt(ids.KindEnvironment, secretReadTestTime(), 45)
		envelope, err := protector.Seal(context.Background(), []byte(value))
		if err != nil {
			t.Fatal(err)
		}
		metadata := envelope.Metadata()
		repository := &fakeEntryReadRepository{
			entry: testkeyvalue.Versioned[testentries.Record]{Record: testentries.Record{
				EnvironmentID: environmentID, CurrentValueGenerationID: generationID,
				Entry: core.EnvEntry{ID: entryID, Kind: core.EntryKindEnv, Key: "PASSWORD",
					Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"all"}, Secret: true},
			}},
			secretValue: testentryvalues.SecretGeneration{
				EnvironmentID: environmentID, EntryID: entryID, GenerationID: generationID,
				EnvelopeVersion: uint8(metadata.Version), Cipher: string(metadata.Cipher),
				DigestAlgorithm: string(metadata.Digest.Algorithm), CiphertextSHA256: metadata.Digest.Value,
				Ciphertext: envelope.Ciphertext(),
			},
		}
		service, err := NewReadService(repository, protector)
		if err != nil {
			t.Fatal(err)
		}
		empty, err := service.SecretValueEmpty(context.Background(), repository.entry.Record)
		if err != nil || empty != (value == "") {
			t.Fatalf("SecretValueEmpty(%d) = %t, %v", index, empty, err)
		}
		if !allBytesZero(repository.secretValue.Ciphertext) {
			t.Fatal("SecretValueEmpty retained repository ciphertext")
		}
	}
}

// Rationale: the value endpoint is a secret-only exception; allowing it for plain Entries would
// create a second read representation and blur the accepted desired-state storage boundary.
func TestEntryRevealRejectsPlainEntry(t *testing.T) {
	t.Parallel()
	entryID := ids.NewAt(ids.KindEnvEntry, secretReadTestTime(), 25)
	repository := &fakeEntryReadRepository{entry: testkeyvalue.Versioned[testentries.Record]{Record: testentries.Record{
		Entry: core.EnvEntry{ID: entryID},
	}}}
	service, err := NewReadService(repository, secretReadTestProtector(t))
	if err != nil {
		t.Fatalf("NewReadService() error = %v", err)
	}
	if value, revealErr := service.RevealEntry(context.Background(), entryID); value != "" ||
		!errors.Is(revealErr, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("RevealEntry() = %q, %v, want validation failure", value, revealErr)
	}
}
