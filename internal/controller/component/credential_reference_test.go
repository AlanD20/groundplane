package component

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotencyowner "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type credentialHierarchyStub struct {
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
}

func (stub credentialHierarchyStub) GetEnvironment(
	_ context.Context,
	_ string,
) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	return stub.environment, nil
}

type credentialSecretsStub struct {
	secret testkeyvalue.Versioned[testsecrets.Record]
}

func (stub credentialSecretsStub) GetSecret(
	_ context.Context,
	_ string,
) (testkeyvalue.Versioned[testsecrets.Record], error) {
	return stub.secret, nil
}

type credentialCreatorStub struct {
	request apiTypes.SecretCreateRequest
	secret  apiTypes.Secret
}

func (stub *credentialCreatorStub) CreateSecret(
	_ context.Context,
	request apiTypes.SecretCreateRequest,
	_ string,
) (testidempotencyowner.IdempotencyResponse, error) {
	stub.request = request
	body, _ := json.Marshal(stub.secret)
	return testidempotencyowner.IdempotencyResponse{Status: http.StatusCreated, Body: body}, nil
}

func TestCredentialReferenceResolverCreatesProjectSecretWithoutReturningToken(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	projectID := ids.NewAt(ids.KindProject, now, 2)
	secretID := ids.NewAt(ids.KindSecret, now, 3)
	creator := &credentialCreatorStub{secret: apiTypes.Secret{ID: secretID, ProjectID: projectID}}
	resolver, err := NewCredentialReferenceResolver(
		credentialHierarchyStub{
			environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
				Record: testhierarchy.EnvironmentRecord{ID: environmentID, ProjectID: projectID},
			},
		},
		credentialSecretsStub{},
		creator,
	)
	if err != nil {
		t.Fatalf("NewCredentialReferenceResolver() error = %v", err)
	}
	resolved, err := resolver.ResolveCredentialReference(
		context.Background(), environmentID,
		OpaqueSecretReferenceInput{Mode: "new", Name: "tunnel-token", Value: "plaintext-token"},
		"idem-key",
	)
	if err != nil {
		t.Fatalf("ResolveCredentialReference() error = %v", err)
	}
	if resolved != secretID || creator.request.ProjectID != projectID || creator.request.Value != "plaintext-token" {
		t.Fatalf("created secret request/result = %+v, %q", creator.request, resolved)
	}
}

func TestCredentialReferenceResolverAcceptsOnlyProjectOrPlatformEnvSecret(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	environmentID := ids.NewAt(ids.KindEnvironment, now, 4)
	projectID := ids.NewAt(ids.KindProject, now, 5)
	secretID := ids.NewAt(ids.KindSecret, now, 6)
	resolver, err := NewCredentialReferenceResolver(
		credentialHierarchyStub{
			environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
				Record: testhierarchy.EnvironmentRecord{ID: environmentID, ProjectID: projectID},
			},
		},
		credentialSecretsStub{
			secret: testkeyvalue.Versioned[testsecrets.Record]{Record: testsecrets.Record{Secret: core.Secret{
				ID: secretID, Scope: core.SecretScopeProject, ProjectID: projectID, Kind: core.SecretKindEnvVar,
			}}},
		},
		&credentialCreatorStub{},
	)
	if err != nil {
		t.Fatalf("NewCredentialReferenceResolver() error = %v", err)
	}
	if resolved, err := resolver.ResolveCredentialReference(
		context.Background(), environmentID,
		OpaqueSecretReferenceInput{Mode: "existing", SecretID: secretID}, "idem-key",
	); err != nil || resolved != secretID {
		t.Fatalf("ResolveCredentialReference(existing) = %q, %v", resolved, err)
	}
}
