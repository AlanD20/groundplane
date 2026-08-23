package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: Entry edit exposes exactly source and exposure; every identity,
// destination, storage-class, and file-ownership field remains immutable.
func TestPrepareEntryEditPreservesImmutableEntryShape(t *testing.T) {
	t.Parallel()

	uid := uint32(1000)
	gid := uint32(1001)
	current := core.EnvEntry{
		ID:       "ent_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Kind:     core.EntryKindFile,
		Path:     "config/app.ini",
		UID:      &uid,
		GID:      &gid,
		Source:   core.EntrySource{Kind: core.SourceLiteral, Literal: "old"},
		Exposure: []string{"api"},
		Secret:   true,
	}
	desired, err := prepareEntryEdit(current, apiTypes.EntryEditRequest{
		Source:   apiTypes.EntrySource{Kind: "literal", Literal: "new"},
		Exposure: []string{"worker", "api"},
	})
	if err != nil {
		t.Fatalf("prepareEntryEdit() error = %v", err)
	}
	if desired.ID != current.ID || desired.Kind != current.Kind || desired.Key != current.Key ||
		desired.Path != current.Path || desired.Secret != current.Secret ||
		desired.UID != current.UID || desired.GID != current.GID {
		t.Fatalf("prepareEntryEdit() changed immutable shape: %#v", desired)
	}
	if desired.Source.Kind != core.SourceLiteral || desired.Source.Literal != "new" ||
		!reflect.DeepEqual(desired.Exposure, []string{"api", "worker"}) {
		t.Fatalf("prepareEntryEdit() mutable state = %#v", desired)
	}
}

// Rationale: secret edit plaintext is transient generation input only; the
// durable primary, public response, and replay marker must remain redacted.
func TestEntryEditPublishesRedactedGenerationAndReplayTarget(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 23, 4, 20, 0, 0, time.UTC)
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	projectID := "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	entryID := ids.NewAt(ids.KindEnvEntry, now, 1)
	repository := &entryEditRepositoryFake{
		entry: etcd.Versioned[etcd.EntryRecord]{
			Record: etcd.EntryRecord{
				EnvironmentID: environmentID,
				Entry: core.EnvEntry{
					ID: entryID, Kind: core.EntryKindEnv, Key: "TOKEN",
					Source:   core.EntrySource{Kind: core.SourceLiteral},
					Exposure: []string{"all"}, Secret: true,
				},
				CurrentValueGenerationID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			},
			Revision: 10, ReadRevision: 10,
		},
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record:   etcd.EnvironmentRecord{ID: environmentID, ProjectID: projectID},
			Revision: 11, ReadRevision: 11,
		},
		project: etcd.Versioned[etcd.ProjectRecord]{
			Record: etcd.ProjectRecord{ID: projectID}, Revision: 12, ReadRevision: 12,
		},
	}
	generator := &entryEditGeneratorFake{}
	idempotency := &entryEditIdempotencyFake{
		evidence: entryEditTestEvidence(),
		known:    idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
	}
	service, err := newEntryEditService(repository, generator, idempotency)
	if err != nil {
		t.Fatalf("newEntryEditService() error = %v", err)
	}
	service.now = func() time.Time { return now }

	response, err := service.EditEntry(context.Background(), entryID, apiTypes.EntryEditRequest{
		Source:   apiTypes.EntrySource{Kind: "literal", Literal: "private-value"},
		Exposure: []string{"all"},
	}, "entry-edit-key-0001")
	if err != nil {
		t.Fatalf("EditEntry() error = %v", err)
	}
	if response.Status != 200 || strings.Contains(string(response.Body), "private-value") {
		t.Fatalf("EditEntry() response = %#v", response)
	}
	if generator.entry.Source.Literal != "private-value" {
		t.Fatalf("generator Entry = %#v", generator.entry)
	}
	if repository.desired.Source.Literal != "" || repository.generationID == "" ||
		!strings.HasPrefix(repository.generationID, "cfg_") {
		t.Fatalf(
			"repository replacement = %#v generation %q",
			repository.desired,
			repository.generationID,
		)
	}
	if repository.marker.ReplayTarget == nil ||
		repository.marker.ReplayTarget.Kind != etcd.IdempotencyReplayTargetEntry ||
		repository.marker.ReplayTarget.ID != entryID {
		t.Fatalf("repository marker = %#v", repository.marker)
	}
}

type entryEditRepositoryFake struct {
	entry        etcd.Versioned[etcd.EntryRecord]
	environment  etcd.Versioned[etcd.EnvironmentRecord]
	project      etcd.Versioned[etcd.ProjectRecord]
	desired      core.EnvEntry
	generationID string
	marker       etcd.IdempotencyMarker
}

func (repository *entryEditRepositoryFake) GetEntry(
	context.Context,
	string,
) (etcd.Versioned[etcd.EntryRecord], error) {
	return repository.entry, nil
}

func (repository *entryEditRepositoryFake) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return repository.environment, nil
}

func (repository *entryEditRepositoryFake) GetProject(
	context.Context,
	string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return repository.project, nil
}

func (*entryEditRepositoryFake) ListServices(
	context.Context,
	string,
	etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	return etcd.Page[etcd.ServiceRecord]{}, nil
}

func (repository *entryEditRepositoryFake) ReplaceEntryIdempotent(
	_ context.Context,
	_ etcd.Versioned[etcd.EnvironmentRecord],
	_ etcd.Versioned[etcd.ProjectRecord],
	_ etcd.Versioned[etcd.EntryRecord],
	desired core.EnvEntry,
	generationID string,
	_ etcd.EntryValueGeneration,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.desired = desired
	repository.generationID = generationID
	repository.marker = marker
	return etcd.IdempotencyTransactionResult{}, nil
}

type entryEditGeneratorFake struct {
	entry core.EnvEntry
}

func (generator *entryEditGeneratorFake) Generate(
	_ context.Context,
	_ string,
	_ string,
	entry core.EnvEntry,
	_ string,
	_ time.Time,
) (etcd.EntryValueGeneration, error) {
	generator.entry = entry
	return etcd.EntryValueGeneration{}, nil
}

type entryEditIdempotencyFake struct {
	evidence entryEditEvidence
	known    idempotentintent.Resolution
}

func (idempotency *entryEditIdempotencyFake) Prepare(
	context.Context,
	string,
	string,
	entryEditInput,
) (entryEditEvidence, error) {
	return idempotency.evidence, nil
}

func (*entryEditIdempotencyFake) ResolveReplayLocator(
	context.Context,
	etcd.IdempotencyReplayTarget,
	string,
	string,
	string,
) (etcd.IdempotencyLocator, bool, error) {
	return etcd.IdempotencyLocator{}, false, nil
}

func (*entryEditIdempotencyFake) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	entryEditEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotentintent.Resolution{}, false, nil
}

func (idempotency *entryEditIdempotencyFake) ResolveKnown(
	context.Context,
	entryEditEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.known, nil
}

func (*entryEditIdempotencyFake) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	entryEditEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{}, nil
}

func entryEditTestEvidence() entryEditEvidence {
	ciphertext := []byte("protected-entry-edit-intent")
	digest := sha256.Sum256(ciphertext)
	return entryEditEvidence{durable: etcd.ProtectedIntentRecord{
		EnvelopeVersion:  1,
		Cipher:           "age-x25519",
		DigestAlgorithm:  "sha256",
		CiphertextDigest: hex.EncodeToString(digest[:]),
		Ciphertext:       ciphertext,
	}}
}
