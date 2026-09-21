package attachments

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type attachFactReadRecordStub struct {
	record testkeyvalue.Versioned[testattachments.Record]
}

func (stub attachFactReadRecordStub) GetAttach(
	context.Context,
	string,
) (testkeyvalue.Versioned[testattachments.Record], error) {
	return stub.record, nil
}

type attachFactValueStub struct {
	environmentID string
	reference     core.FactRef
}

func (stub *attachFactValueStub) ResolveFact(
	_ context.Context,
	environmentID string,
	reference core.FactRef,
	_ bool,
	consume secretvalue.PlaintextConsumer,
) error {
	stub.environmentID = environmentID
	stub.reference = reference
	return consume([]byte("postgres://ready"))
}

// Rationale: the public reveal boundary must resolve the owning Environment from the durable Attach
// instead of trusting caller scope, while preserving the selected grant and fact identity.
func TestAttachFactReadUsesDurableEnvironmentScope(t *testing.T) {
	t.Parallel()
	resolver := &attachFactValueStub{}
	service, err := NewFactReadService(attachFactReadRecordStub{record: testkeyvalue.Versioned[testattachments.Record]{
		Record: testattachments.Record{
			ID:            "att_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
	}}, resolver)
	if err != nil {
		t.Fatalf("NewFactReadService() error = %v", err)
	}
	value, err := service.RevealAttachFact(
		context.Background(),
		"att_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"att_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		"pg16_URL",
	)
	if err != nil || value != "postgres://ready" ||
		resolver.environmentID != "env_01ARZ3NDEKTSV4RRFFQ69G5FAV" ||
		resolver.reference.Grant != "att_01ARZ3NDEKTSV4RRFFQ69G5FAW" || resolver.reference.Key != "pg16_URL" {
		t.Fatalf("RevealAttachFact() = %q, %v / %#v", value, err, resolver)
	}
}
