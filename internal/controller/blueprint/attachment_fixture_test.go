package blueprint

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/adapters/custom"
	"github.com/AlanD20/groundplane/internal/adapters/postgres"
	"github.com/AlanD20/groundplane/internal/adapters/valkey"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func registerAdapters() {
	if _, registered := adapters.Get("postgres"); !registered {
		postgres.Register()
	}
	if _, registered := adapters.Get("valkey"); !registered {
		valkey.Register()
	}
	if _, registered := adapters.Get("custom"); !registered {
		custom.Register()
	}
}

type blueprintTestCrypt struct{}

func (blueprintTestCrypt) Seal(_ context.Context, plaintext []byte) ([]byte, error) {
	return append([]byte(nil), plaintext...), nil
}

func (blueprintTestCrypt) Open(_ context.Context, ciphertext []byte) ([]byte, error) {
	return append([]byte(nil), ciphertext...), nil
}

type blueprintAttachFactRepository struct {
	records   map[string]testkeyvalue.Versioned[testattachments.Record]
	facts     testattachments.EncryptedFacts
	factsByID map[string]testattachments.EncryptedFacts
	factReads int
}

func (repository *blueprintAttachFactRepository) ResolveAttach(
	_ context.Context,
	environmentID string,
	reference string,
) (testkeyvalue.Versioned[testattachments.Record], error) {
	record, ok := repository.records[reference]
	if !ok {
		return testkeyvalue.Versioned[testattachments.Record]{}, errs.New(
			errs.KindAttachNotFound,
			"Attach was not found",
		)
	}
	if record.Record.EnvironmentID != environmentID {
		return testkeyvalue.Versioned[testattachments.Record]{}, errs.New(
			errs.KindScopeUnauthorized,
			"Attach scope mismatch",
		)
	}
	return record, nil
}

func (repository *blueprintAttachFactRepository) GetAttachFacts(
	_ context.Context,
	owner testkeyvalue.Versioned[testattachments.Record],
) (testattachments.EncryptedFacts, bool, error) {
	repository.factReads++
	result := repository.facts
	if repository.factsByID != nil {
		result = repository.factsByID[owner.Record.ID]
	}
	if len(result.Ciphertext) == 0 {
		return testattachments.EncryptedFacts{}, false, nil
	}
	result.Ciphertext = append([]byte(nil), result.Ciphertext...)
	return result, true, nil
}

// Supply ready bindings so candidate sealing can be checked through the actual
// fact reader. This does not execute or acknowledge provisioning.
func candidateFactService(t *testing.T, prepared preparedBlueprintAttaches,
	protector *secretvalue.Protector,
) *attachments.FactService {
	t.Helper()
	repository := &blueprintAttachFactRepository{
		records:   make(map[string]testkeyvalue.Versioned[testattachments.Record]),
		factsByID: make(map[string]testattachments.EncryptedFacts),
	}
	for _, candidate := range prepared.candidates {
		record, err := testattachments.MarkAttachProvisioning(candidate.Record, candidate.Record.TaskID)
		if err != nil {
			t.Fatal(err)
		}
		record, err = testattachments.CompleteAttachProvisioning(record, record.TaskID, true)
		if err != nil {
			t.Fatal(err)
		}
		versioned := testkeyvalue.Versioned[testattachments.Record]{Record: record, Revision: 1, ReadRevision: 1}
		repository.records[record.Name], repository.records[record.ID] = versioned, versioned
		if candidate.Facts != nil {
			repository.factsByID[record.ID] = *candidate.Facts
		}
	}
	service, err := attachments.NewFactService(repository, protector)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

type attachFactTestAdapter struct{}

func (attachFactTestAdapter) Key() string                         { return "test:1" }
func (attachFactTestAdapter) Label() string                       { return "Test" }
func (attachFactTestAdapter) DefaultImage(string) (string, error) { return "test:1", nil }
func (attachFactTestAdapter) CredentialIdentityLimit() int        { return 63 }
func (attachFactTestAdapter) FactsPrefix() string                 { return "test_" }
func (attachFactTestAdapter) URLScheme() string                   { return "pgsql://" }
func (attachFactTestAdapter) Port() string                        { return "5432" }
func (attachFactTestAdapter) Custom() bool                        { return false }
func (attachFactTestAdapter) SupportsGrants() bool                { return true }
func (attachFactTestAdapter) SupportsAuthenticationModes() bool   { return false }
func (attachFactTestAdapter) FactSchema(core.BackingAuthentication) []adapters.FactDefinition {
	return []adapters.FactDefinition{
		{Field: adapters.FactDatabase},
		{Field: adapters.FactPassword, Secret: true},
		{Field: adapters.FactURL, Secret: true},
	}
}
func (attachFactTestAdapter) ProvisionSteps(adapters.Input) []adapters.Step { return nil }
func (attachFactTestAdapter) GrantSteps(adapters.Input) []adapters.Step     { return nil }
func (attachFactTestAdapter) RevokeSteps(adapters.Input) []adapters.Step    { return nil }
func (attachFactTestAdapter) DetachSteps(adapters.Input) []adapters.Step    { return nil }
func (attachFactTestAdapter) BackupStrategy() adapters.BackupStrategy {
	return adapters.BackupStrategy{}
}

func readyAttachRecord(
	t *testing.T,
	id string,
	environmentID string,
	name string,
	backingProjectID string,
	backingEnvironmentID string,
	backingServiceID string,
	backingNetworkID string,
	serviceID string,
	grantIDs []string,
	factSets []testattachments.FactSetMetadata,
	now time.Time,
	seed int64,
) testattachments.Record {
	t.Helper()
	record, err := testattachments.NewPendingAttachRecord(
		id,
		environmentID,
		name,
		backingProjectID,
		backingEnvironmentID,
		backingServiceID,
		backingNetworkID,
		serviceID,
		id,
		grantIDs,
		factSets,
		ids.NewAt(ids.KindTask, now, seed),
		now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord() error = %v", err)
	}
	record, err = testattachments.MarkAttachProvisioning(record, record.TaskID)
	if err != nil {
		t.Fatalf("MarkAttachProvisioning() error = %v", err)
	}
	record, err = testattachments.CompleteAttachProvisioning(record, record.TaskID, true)
	if err != nil {
		t.Fatalf("CompleteAttachProvisioning() error = %v", err)
	}
	return record
}
