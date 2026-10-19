package etcd

import (
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Establish resource metadata for reader/lifecycle checks, not Task acceptance
// or execution. Native public acceptance is exercised in application integration.
func seedAttachMetadata(t *testing.T, repository *AttachRepository, record attachments.Record,
	facts *attachments.EncryptedFacts,
) keyvalue.Versioned[attachments.Record] {
	t.Helper()
	marker := testDirectMarker()
	marker.CreatedAt, marker.UpdatedAt = record.CreatedAt, record.CreatedAt
	marker.Locator = idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID: record.EnvironmentID, Method: http.MethodPost, Route: "/attaches", Key: record.ID}
	desired, err := prepareAttachDesiredAuthoring(t.Context(), repository.store, record, "", false, marker)
	if err != nil {
		t.Fatal(err)
	}
	defer clearDesiredMutationFragment(desired)
	value, err := attachments.EncodeAttachRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: attachments.AttachKey(record.ID), Value: value}}
	defer func() { keyvalue.ClearMutationValues(mutations) }()
	indexes := []string{
		attachments.AttachNameKey(record.EnvironmentID, record.Name),
		attachments.AttachOwnerKey(record.EnvironmentID, record.ID),
		attachments.AttachServiceKey(record.ServiceID, record.ID),
		attachments.AttachBackingServiceKey(record.BackingServiceID, record.ID),
		attachments.AttachBackingProjectKey(record.BackingProjectID, record.ID),
	}
	if !record.OwnsCredential() {
		indexes = append(indexes, attachments.AttachCredentialByKey(record.CredentialAttachID, record.ID))
	}
	for _, grant := range record.GrantAttachIDs {
		indexes = append(indexes, attachments.AttachGrantedByKey(grant, record.ID))
	}
	for _, key := range indexes {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: []byte(record.ID)})
	}
	if len(record.GrantAttachIDs) != 0 {
		value, err := attachments.EncodeAttachDependentGrantIndex(record.ID, record.GrantAttachIDs)
		if err != nil {
			t.Fatal(err)
		}
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
			Key: attachments.AttachDependentGrantKey(record.ID), Value: value})
	}
	if facts != nil {
		value, err := attachments.EncodeAttachEncryptedFacts(*facts)
		if err != nil {
			t.Fatal(err)
		}
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
			Key: attachments.AttachFactsKey(record.ID), Value: value})
	}
	mutations = append(mutations, desired.mutations...)
	result, err := repository.store.Transact(t.Context(), desired.conditions, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Attach metadata: %v", err)
	}
	stored, err := repository.GetAttach(t.Context(), record.ID)
	if err != nil {
		t.Fatal(err)
	}
	return stored
}
