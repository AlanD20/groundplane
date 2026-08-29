package app

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type componentCredentialHierarchy interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
}

type componentCredentialSecrets interface {
	GetSecret(context.Context, string) (etcd.Versioned[etcd.SecretRecord], error)
}

type componentCredentialCreator interface {
	CreateSecret(context.Context, apiTypes.SecretCreateRequest, string) (etcd.IdempotencyResponse, error)
}

type componentCloudflareCredentialResolver struct {
	hierarchy componentCredentialHierarchy
	secrets   componentCredentialSecrets
	creator   componentCredentialCreator
}

func newComponentCloudflareCredentialResolver(
	hierarchy componentCredentialHierarchy,
	secrets componentCredentialSecrets,
	creator componentCredentialCreator,
) (*componentCloudflareCredentialResolver, error) {
	if hierarchy == nil || secrets == nil || creator == nil {
		return nil, errs.New(errs.KindInternal, "Cloudflare Tunnel credential resolver is not configured")
	}
	return &componentCloudflareCredentialResolver{hierarchy: hierarchy, secrets: secrets, creator: creator}, nil
}

func (resolver *componentCloudflareCredentialResolver) ResolveCloudflareTunnelCredential(
	ctx context.Context,
	environmentID string,
	credential apiTypes.CloudflareTunnelCredentialInput,
	idempotencyKey string,
) (string, error) {
	if ctx == nil || ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return "", errs.New(errs.KindValidationFailed, "Cloudflare Tunnel credential config is invalid")
	}
	environment, err := resolver.hierarchy.GetEnvironment(ctx, environmentID)
	if err != nil { return "", err }
	switch credential.Mode {
	case "existing":
		if credential.SecretID == "" || credential.SecretName != "" || credential.Token != "" {
			return "", errs.New(errs.KindValidationFailed, "Existing Cloudflare credential accepts only mode and secret_id")
		}
		if ids.Validate(ids.KindSecret, credential.SecretID) != nil {
			return "", errs.New(errs.KindValidationFailed, "Cloudflare Tunnel secret_id is invalid")
		}
		if err := resolver.validateSelection(ctx, environment.Record.ProjectID, credential.SecretID); err != nil {
			return "", err
		}
		return credential.SecretID, nil
	case "new":
		if credential.SecretID != "" || credential.SecretName == "" || credential.Token == "" {
			return "", errs.New(errs.KindValidationFailed, "New Cloudflare credential accepts only mode, secret_name, and token")
		}
		response, err := resolver.creator.CreateSecret(ctx, apiTypes.SecretCreateRequest{
			ProjectID: environment.Record.ProjectID,
			Key:       credential.SecretName,
			Kind:      string(core.SecretKindEnvVar),
			Value:     credential.Token,
		}, idempotencyKey)
		if err != nil {
			return "", err
		}
		var created apiTypes.Secret
		if response.Status != http.StatusCreated || json.Unmarshal(response.Body, &created) != nil ||
			ids.Validate(ids.KindSecret, created.ID) != nil || created.ProjectID != environment.Record.ProjectID {
			return "", errs.New(errs.KindInternal, "Cloudflare Tunnel Secret creation response is invalid")
		}
		return created.ID, nil
	default:
		return "", errs.New(errs.KindValidationFailed, "Cloudflare Tunnel credential mode is invalid")
	}
}

func (resolver *componentCloudflareCredentialResolver) validateSelection(ctx context.Context, projectID, secretID string) error {
	current, err := resolver.secrets.GetSecret(ctx, secretID)
	if err != nil {
		return err
	}
	secret := current.Record.Secret
	if secret.ID != secretID || secret.Kind != core.SecretKindEnvVar ||
		(secret.Scope == core.SecretScopeProject && secret.ProjectID != projectID) ||
		(secret.Scope != core.SecretScopeProject && secret.Scope != core.SecretScopePlatform) {
		return errs.New(errs.KindValidationFailed, "Cloudflare Tunnel Secret is unavailable in this Project")
	}
	return nil
}
