package blueprint

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/adapters/custom"
	"github.com/AlanD20/groundplane/internal/adapters/postgres16"
	"github.com/AlanD20/groundplane/internal/adapters/valkey9"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func registerAdapters() {
	if _, registered := adapters.Get("postgres:16"); !registered {
		postgres16.Register()
	}
	if _, registered := adapters.Get("valkey:9"); !registered {
		valkey9.Register()
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
	_ testkeyvalue.Versioned[testattachments.Record],
) (testattachments.EncryptedFacts, bool, error) {
	repository.factReads++
	if len(repository.facts.Ciphertext) == 0 {
		return testattachments.EncryptedFacts{}, false, nil
	}
	result := repository.facts
	result.Ciphertext = append([]byte(nil), repository.facts.Ciphertext...)
	return result, true, nil
}

type attachFactTestAdapter struct{}

func (attachFactTestAdapter) Key() string                       { return "test:1" }
func (attachFactTestAdapter) Label() string                     { return "Test" }
func (attachFactTestAdapter) DefaultImage() string              { return "test:1" }
func (attachFactTestAdapter) FactsPrefix() string               { return "test_" }
func (attachFactTestAdapter) URLScheme() string                 { return "pgsql://" }
func (attachFactTestAdapter) Port() string                      { return "5432" }
func (attachFactTestAdapter) Custom() bool                      { return false }
func (attachFactTestAdapter) SupportsGrants() bool              { return true }
func (attachFactTestAdapter) SupportsAuthenticationModes() bool { return false }
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
