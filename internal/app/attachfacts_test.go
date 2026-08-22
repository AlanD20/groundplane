package app

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/ids"
	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: sealing must bind sorted public facts and private retry identity
// to one opaque envelope without retaining caller passwords.
func TestAttachFactServiceSealsAndResolvesGrantFacts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	crypt := attachFactTestCrypt{}
	protector, err := secretvalue.NewProtector(crypt, crypt)
	if err != nil {
		t.Fatalf("NewProtector() error = %v", err)
	}
	repository := &attachFactTestRepository{records: make(map[string]etcd.Versioned[etcd.AttachRecord])}
	service, err := NewAttachFactService(repository, protector)
	if err != nil {
		t.Fatalf("NewAttachFactService() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	backingProjectID := ids.NewAt(ids.KindProject, now, 2)
	backingEnvironmentID := ids.NewAt(ids.KindEnvironment, now, 3)
	backingServiceID := ids.NewAt(ids.KindService, now, 4)
	backingNetworkID := ids.NewAt(ids.KindNetwork, now, 40)
	serviceID := ids.NewAt(ids.KindService, now, 5)
	ownerID := ids.NewAt(ids.KindAttach, now, 6)
	grantID := ids.NewAt(ids.KindAttach, now, 7)
	password := []byte("correct-horse")
	metadata, encrypted, err := service.SealFactSets(
		ctx,
		ownerID,
		attachFactTestAdapter{},
		adapters.FactParams{
			Host: "postgres", Port: "5432", Database: "appdb", Role: "app", Password: password,
		},
		[]AttachGrantFactParams{{
			AttachID: grantID,
			Params: adapters.FactParams{
				Host: "postgres", Port: "5432", Database: "reporting", Role: "app", Password: password,
			},
		}},
	)
	if err != nil {
		t.Fatalf("SealFactSets() error = %v", err)
	}
	if !slices.Equal(password, []byte("correct-horse")) {
		t.Fatalf("SealFactSets() mutated caller password: %q", password)
	}
	if encrypted == nil || len(metadata) != 2 || metadata[1].GrantAttachID != grantID {
		t.Fatalf("SealFactSets() = %#v, %#v", metadata, encrypted)
	}
	taskID := ids.NewAt(ids.KindTask, now, 9)
	pending, err := etcd.NewPendingAttachRecord(
		ownerID, environmentID, "api-db", backingProjectID, backingEnvironmentID, backingServiceID,
		backingNetworkID, []string{serviceID}, []string{grantID}, metadata, taskID, now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord() error = %v", err)
	}
	repository.facts = *encrypted
	var taskIdentity controllerpkg.AttachPlanIdentity
	err = service.ResolveTaskIdentity(
		ctx,
		etcd.Versioned[etcd.AttachRecord]{Record: pending, Revision: 9, ReadRevision: 9},
		taskID,
		func(identity controllerpkg.AttachPlanIdentity) error {
			taskIdentity = controllerpkg.AttachPlanIdentity{
				Database: identity.Database, Role: identity.Role,
				Password: append([]byte(nil), identity.Password...),
				Grants:   append([]controllerpkg.AttachPlanGrantIdentity(nil), identity.Grants...),
			}
			return nil
		},
	)
	if err != nil || taskIdentity.Database != "appdb" || taskIdentity.Role != "app" ||
		string(taskIdentity.Password) != "correct-horse" || len(taskIdentity.Grants) != 1 ||
		taskIdentity.Grants[0].Database != "reporting" {
		t.Fatalf("ResolveTaskIdentity() = %#v, %v", taskIdentity, err)
	}
	taskIdentity.Clear()
	owner := readyAttachRecord(
		t,
		ownerID,
		environmentID,
		"api-db",
		backingProjectID,
		backingEnvironmentID,
		backingServiceID,
		backingNetworkID,
		serviceID,
		[]string{grantID},
		metadata,
		now,
		10,
	)
	grantMetadata := []etcd.AttachFactSetMetadata{{Facts: metadata[0].Facts}}
	grant := readyAttachRecord(
		t,
		grantID,
		environmentID,
		"reporting-db",
		backingProjectID,
		backingEnvironmentID,
		backingServiceID,
		backingNetworkID,
		serviceID,
		nil,
		grantMetadata,
		now,
		20,
	)
	repository.records["api-db"] = etcd.Versioned[etcd.AttachRecord]{Record: owner, Revision: 11, ReadRevision: 11}
	repository.records[ownerID] = repository.records["api-db"]
	repository.records["reporting-db"] = etcd.Versioned[etcd.AttachRecord]{
		Record:       grant,
		Revision:     12,
		ReadRevision: 12,
	}
	repository.records[grantID] = repository.records["reporting-db"]
	var resolved []byte
	err = service.ResolveFact(ctx, environmentID, core.FactRef{
		Attach: "api-db", Grant: "reporting-db", Key: "test_DATABASE",
	}, false, func(value []byte) error {
		resolved = append([]byte(nil), value...)
		return nil
	})
	if err != nil {
		t.Fatalf("ResolveFact() error = %v", err)
	}
	if string(resolved) != "reporting" {
		t.Fatalf("ResolveFact() value = %q, want reporting", resolved)
	}
	err = service.ResolveFact(ctx, environmentID, core.FactRef{
		Attach: "api-db", Key: "test_PASSWORD",
	}, false, func([]byte) error { return nil })
	kind, _ := errs.KindOf(err)
	if kind != errs.KindValidationFailed {
		t.Fatalf("ResolveFact(non-secret destination) error = %v", err)
	}
}

// Rationale: live Entry resolution must not expose persisted fact bytes while an Attach is pending or failed.
func TestAttachFactServiceRequiresReadyAttach(t *testing.T) {
	t.Parallel()
	crypt := attachFactTestCrypt{}
	protector, err := secretvalue.NewProtector(crypt, crypt)
	if err != nil {
		t.Fatalf("NewProtector() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 16, 0, 0, 0, time.UTC)
	record := readyAttachRecord(
		t,
		ids.NewAt(ids.KindAttach, now, 1),
		ids.NewAt(ids.KindEnvironment, now, 2),
		"api-db",
		ids.NewAt(ids.KindProject, now, 3),
		ids.NewAt(ids.KindEnvironment, now, 4),
		ids.NewAt(ids.KindService, now, 5),
		ids.NewAt(ids.KindNetwork, now, 8),
		ids.NewAt(ids.KindService, now, 6),
		nil,
		[]etcd.AttachFactSetMetadata{{Facts: []etcd.AttachFactDefinition{{Key: "test_DATABASE"}}}},
		now,
		7,
	)
	record.Status = core.AttachFailed
	repository := &attachFactTestRepository{
		records: map[string]etcd.Versioned[etcd.AttachRecord]{
			"api-db": {Record: record, Revision: 1, ReadRevision: 1},
		},
	}
	service, err := NewAttachFactService(repository, protector)
	if err != nil {
		t.Fatalf("NewAttachFactService() error = %v", err)
	}
	err = service.ResolveFact(
		context.Background(),
		record.EnvironmentID,
		core.FactRef{Attach: "api-db", Key: "test_DATABASE"},
		false,
		func([]byte) error { return nil },
	)
	kind, _ := errs.KindOf(err)
	if kind != errs.KindStateConflict || repository.factReads != 0 {
		t.Fatalf("ResolveFact() error = %v, fact reads = %d", err, repository.factReads)
	}
}

type attachFactTestRepository struct {
	records   map[string]etcd.Versioned[etcd.AttachRecord]
	facts     etcd.AttachEncryptedFacts
	factReads int
}

func (repository *attachFactTestRepository) ResolveAttach(
	_ context.Context,
	environmentID string,
	reference string,
) (etcd.Versioned[etcd.AttachRecord], error) {
	record, ok := repository.records[reference]
	if !ok {
		return etcd.Versioned[etcd.AttachRecord]{}, errs.New(errs.KindAttachNotFound, "Attach was not found")
	}
	if record.Record.EnvironmentID != environmentID {
		return etcd.Versioned[etcd.AttachRecord]{}, errs.New(errs.KindScopeUnauthorized, "Attach scope mismatch")
	}
	return record, nil
}

func (repository *attachFactTestRepository) GetAttachFacts(
	_ context.Context,
	_ etcd.Versioned[etcd.AttachRecord],
) (etcd.AttachEncryptedFacts, bool, error) {
	repository.factReads++
	if len(repository.facts.Ciphertext) == 0 {
		return etcd.AttachEncryptedFacts{}, false, nil
	}
	result := repository.facts
	result.Ciphertext = append([]byte(nil), repository.facts.Ciphertext...)
	return result, true, nil
}

type attachFactTestCrypt struct{}

func (attachFactTestCrypt) Seal(_ context.Context, plaintext []byte) ([]byte, error) {
	return append([]byte(nil), plaintext...), nil
}

func (attachFactTestCrypt) Open(_ context.Context, ciphertext []byte) ([]byte, error) {
	return append([]byte(nil), ciphertext...), nil
}

type attachFactTestAdapter struct{}

func (attachFactTestAdapter) Key() string          { return "test:1" }
func (attachFactTestAdapter) Label() string        { return "Test" }
func (attachFactTestAdapter) DefaultImage() string { return "test:1" }
func (attachFactTestAdapter) FactsPrefix() string  { return "test_" }
func (attachFactTestAdapter) URLScheme() string    { return "pgsql://" }
func (attachFactTestAdapter) Port() string         { return "5432" }
func (attachFactTestAdapter) Manual() bool         { return false }
func (attachFactTestAdapter) SupportsGrants() bool { return true }
func (attachFactTestAdapter) FactSchema() []adapters.FactDefinition {
	return []adapters.FactDefinition{
		{Field: adapters.FactDatabase},
		{Field: adapters.FactPassword, Secret: true},
		{Field: adapters.FactURL, Secret: true},
	}
}
func (attachFactTestAdapter) ProvisionSteps(adapters.ProvisionParams) []adapters.Step { return nil }
func (attachFactTestAdapter) GrantSteps(adapters.ProvisionParams) []adapters.Step     { return nil }
func (attachFactTestAdapter) RevokeSteps(adapters.ProvisionParams) []adapters.Step    { return nil }
func (attachFactTestAdapter) DetachSteps(adapters.ProvisionParams) []adapters.Step    { return nil }
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
	factSets []etcd.AttachFactSetMetadata,
	now time.Time,
	seed int64,
) etcd.AttachRecord {
	t.Helper()
	record, err := etcd.NewPendingAttachRecord(
		id,
		environmentID,
		name,
		backingProjectID,
		backingEnvironmentID,
		backingServiceID,
		backingNetworkID,
		[]string{serviceID},
		grantIDs,
		factSets,
		ids.NewAt(ids.KindTask, now, seed),
		now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord() error = %v", err)
	}
	record, err = etcd.MarkAttachProvisioning(record, record.TaskID)
	if err != nil {
		t.Fatalf("MarkAttachProvisioning() error = %v", err)
	}
	record, err = etcd.CompleteAttachProvisioning(record, record.TaskID, true)
	if err != nil {
		t.Fatalf("CompleteAttachProvisioning() error = %v", err)
	}
	return record
}
