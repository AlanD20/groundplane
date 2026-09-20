package component

import (
	"context"
	"encoding/json"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type credentialHierarchy interface {
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
}

type credentialSecrets interface {
	GetSecret(context.Context, string) (etcdstore.Versioned[secretrecord.Record], error)
}

type credentialCreator interface {
	CreateSecret(context.Context, apiTypes.SecretCreateRequest, string) (idempotencyrecord.IdempotencyResponse, error)
}

type OpaqueSecretReferenceInput struct {
	Mode     string
	SecretID string
	Name     string
	Value    string
}

type CredentialReferenceResolver struct {
	hierarchy credentialHierarchy
	secrets   credentialSecrets
	creator   credentialCreator
}

func NewCredentialReferenceResolver(
	hierarchy credentialHierarchy,
	secrets credentialSecrets,
	creator credentialCreator,
) (*CredentialReferenceResolver, error) {
	if hierarchy == nil || secrets == nil || creator == nil {
		return nil, errs.New(errs.KindInternal, "Component credential resolver is not configured")
	}
	return &CredentialReferenceResolver{hierarchy: hierarchy, secrets: secrets, creator: creator}, nil
}

func (resolver *CredentialReferenceResolver) ResolveCredentialReference(
	ctx context.Context,
	environmentID string,
	credential OpaqueSecretReferenceInput,
	idempotencyKey string,
) (string, error) {
	if ctx == nil || ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return "", errs.New(errs.KindValidationFailed, "Component credential reference is invalid")
	}
	environment, err := resolver.hierarchy.GetEnvironment(ctx, environmentID)
	if err != nil {
		return "", err
	}
	switch credential.Mode {
	case "existing":
		if credential.SecretID == "" || credential.Name != "" || credential.Value != "" {
			return "", errs.New(
				errs.KindValidationFailed,
				"Existing Component credential accepts only mode and secret_id",
			)
		}
		if ids.Validate(ids.KindSecret, credential.SecretID) != nil {
			return "", errs.New(errs.KindValidationFailed, "Component credential secret_id is invalid")
		}
		if err := resolver.validateSelection(ctx, environment.Record.ProjectID, credential.SecretID); err != nil {
			return "", err
		}
		return credential.SecretID, nil
	case "new":
		if credential.SecretID != "" || credential.Name == "" || credential.Value == "" {
			return "", errs.New(
				errs.KindValidationFailed,
				"New Component credential accepts only mode, name, and value",
			)
		}
		response, err := resolver.creator.CreateSecret(ctx, apiTypes.SecretCreateRequest{
			ProjectID: environment.Record.ProjectID,
			Key:       credential.Name,
			Kind:      string(core.SecretKindEnvVar),
			Value:     credential.Value,
		}, idempotencyKey)
		if err != nil {
			return "", err
		}
		var created apiTypes.Secret
		if response.Status != http.StatusCreated || json.Unmarshal(response.Body, &created) != nil ||
			ids.Validate(ids.KindSecret, created.ID) != nil || created.ProjectID != environment.Record.ProjectID {
			return "", errs.New(errs.KindInternal, "Component credential Secret creation response is invalid")
		}
		return created.ID, nil
	default:
		return "", errs.New(errs.KindValidationFailed, "Component credential mode is invalid")
	}
}

func (resolver *CredentialReferenceResolver) validateSelection(ctx context.Context, projectID, secretID string) error {
	current, err := resolver.secrets.GetSecret(ctx, secretID)
	if err != nil {
		return err
	}
	secret := current.Record.Secret
	if secret.ID != secretID || secret.Kind != core.SecretKindEnvVar ||
		(secret.Scope == core.SecretScopeProject && secret.ProjectID != projectID) ||
		(secret.Scope != core.SecretScopeProject && secret.Scope != core.SecretScopePlatform) {
		return errs.New(errs.KindValidationFailed, "Component credential Secret is unavailable in this Project")
	}
	return nil
}
