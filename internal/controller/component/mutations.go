package component

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

type mutationRepository interface {
	GetComponent(context.Context, string) (etcd.Versioned[etcd.ComponentRecord], error)
}

type mutationBlueprintRepository interface {
	GetEnvironmentBlueprintHead(context.Context, string) (etcd.Versioned[etcd.EnvironmentBlueprintHead], bool, error)
	GetEnvironmentBlueprintRevision(context.Context, string, string) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error)
}

type blueprintApplier interface {
	ApplyComponentBlueprint(context.Context, string, string, core.BlueprintBundle, string) (etcd.IdempotencyResponse, error)
}

type credentialReferenceResolver interface {
	ResolveCredentialReference(context.Context, string, OpaqueSecretReferenceInput, string) (string, error)
}

type platformConfigMutator interface {
	EnablePlatformComponent(context.Context, string, string) (etcd.IdempotencyResponse, error)
	DisablePlatformComponent(context.Context, string, string) (etcd.IdempotencyResponse, error)
	UpdatePlatformComponent(context.Context, string, string) (etcd.IdempotencyResponse, error)
	ReplacePlatformComponentConfig(
		context.Context,
		string,
		apiTypes.ComponentConfigMutationRequest,
		string,
	) (etcd.IdempotencyResponse, error)
}

type MutationService struct {
	components  mutationRepository
	blueprints  mutationBlueprintRepository
	applier     blueprintApplier
	credentials credentialReferenceResolver
	platform    platformConfigMutator
}

func NewMutationService(
	components mutationRepository,
	blueprints mutationBlueprintRepository,
	applier blueprintApplier,
	credentials credentialReferenceResolver,
	platform platformConfigMutator,
) (*MutationService, error) {
	if components == nil || blueprints == nil || applier == nil || credentials == nil || platform == nil {
		return nil, errs.New(errs.KindInternal, "Component mutation dependencies are not configured")
	}
	return &MutationService{
		components:  components,
		blueprints:  blueprints,
		applier:     applier,
		credentials: credentials,
		platform:    platform,
	}, nil
}

func (service *MutationService) EnableComponent(
	ctx context.Context,
	componentID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	current, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if current.Record.Desired.Owner == core.ComponentOwnerPlatform {
		return service.platform.EnablePlatformComponent(ctx, componentID, idempotencyKey)
	}
	return service.mutateComponent(ctx, componentID, idempotencyKey, func(spec *yaml.Node) error {
		return replaceYAMLMappingValue(spec, "enabled", scalarNode("!!bool", strconv.FormatBool(true)))
	})
}

func (service *MutationService) DisableComponent(
	ctx context.Context,
	componentID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	current, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if current.Record.Desired.Owner == core.ComponentOwnerPlatform {
		return service.platform.DisablePlatformComponent(ctx, componentID, idempotencyKey)
	}
	return service.mutateComponent(ctx, componentID, idempotencyKey, func(spec *yaml.Node) error {
		return replaceYAMLMappingValue(spec, "enabled", scalarNode("!!bool", strconv.FormatBool(false)))
	})
}

func (service *MutationService) UpdateComponent(
	ctx context.Context,
	componentID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	current, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if current.Record.Desired.Owner == core.ComponentOwnerPlatform {
		return service.platform.UpdatePlatformComponent(ctx, componentID, idempotencyKey)
	}
	return service.mutateComponent(ctx, componentID, idempotencyKey, func(*yaml.Node) error { return nil })
}

func (service *MutationService) SetComponentConfig(
	ctx context.Context,
	componentID string,
	request apiTypes.ComponentConfigMutationRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if err := request.Config.Validate(); err != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, err.Error())
	}
	current, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if current.Record.Desired.Owner == core.ComponentOwnerPlatform {
		return service.platform.ReplacePlatformComponentConfig(ctx, componentID, request, idempotencyKey)
	}
	var publicConfig apiTypes.ComponentConfig
	var mutate func(*yaml.Node) error
	if current.Record.Desired.Kind == core.ComponentKindEdgeCloudflare {
		if request.Config.CloudflareTunnel == nil {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindValidationFailed,
				"Cloudflare Tunnel config accepts only credential",
			)
		}
		credential := request.Config.CloudflareTunnel.Credential
		secretID, err := service.credentials.ResolveCredentialReference(
			ctx,
			current.Record.Desired.OwnerID,
			OpaqueSecretReferenceInput{
				Mode: credential.Mode, SecretID: credential.SecretID,
				Name: credential.SecretName, Value: credential.Token,
			},
			idempotencyKey,
		)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		publicConfig = apiTypes.ComponentConfig{CloudflareTunnel: &apiTypes.CloudflareTunnelComponentConfig{SecretID: secretID}}
		mutate = func(spec *yaml.Node) error {
			var settings yaml.Node
			if err := settings.Encode(map[string]string{"secret_id": secretID}); err != nil {
				return errs.New(errs.KindValidationFailed, "Component config is invalid")
			}
			return replaceYAMLMappingValue(spec, "settings", &settings)
		}
	} else {
		if current.Record.Desired.Kind != core.ComponentKindIngressCaddy || request.Config.Caddy == nil ||
			request.Config.Caddy.ZoneID == "" {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindValidationFailed,
				"Caddy config requires zone_id and accepts only Caddy fields",
			)
		}
		publicConfig = apiTypes.ComponentConfig{Caddy: &apiTypes.CaddyComponentConfig{
			ZoneID: request.Config.Caddy.ZoneID, CaddyfileTemplate: request.Config.Caddy.CaddyfileTemplate,
		}}
		mutate = func(spec *yaml.Node) error {
			var settings yaml.Node
			if err := settings.Encode(map[string]string{"zone_id": request.Config.Caddy.ZoneID}); err != nil {
				return errs.New(errs.KindValidationFailed, "Component config is invalid")
			}
			if err := replaceYAMLMappingValue(spec, "settings", &settings); err != nil {
				return err
			}
			if request.Config.Caddy.CaddyfileTemplate == "" {
				removeYAMLMappingValue(spec, "implementation_config")
				return nil
			}
			var implementation yaml.Node
			if err := implementation.Encode(map[string]string{
				"caddyfile_template": request.Config.Caddy.CaddyfileTemplate,
			}); err != nil {
				return errs.New(errs.KindValidationFailed, "Component config is invalid")
			}
			return replaceYAMLMappingValue(spec, "implementation_config", &implementation)
		}
	}
	response, err := service.mutateComponent(ctx, componentID, idempotencyKey, mutate)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	var accepted apiTypes.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil || accepted.TaskID == "" {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Component Blueprint Task response is invalid")
	}
	body, err := json.Marshal(apiTypes.ComponentConfigMutationResult{
		Resource: publicConfig, ReconcileTaskID: &accepted.TaskID,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	return etcd.IdempotencyResponse{Status: http.StatusOK, ContentKind: "application/json", Body: body}, nil
}

func (service *MutationService) mutateComponent(
	ctx context.Context,
	componentID string,
	idempotencyKey string,
	mutate func(*yaml.Node) error,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Component mutation context is required")
	}
	component, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if component.Record.Desired.Owner != core.ComponentOwnerEnvironment {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Platform Component actions require their dedicated lifecycle API",
		)
	}
	if component.Record.Desired.Kind != core.ComponentKindIngressCaddy &&
		component.Record.Desired.Kind != core.ComponentKindEdgeCloudflare {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "Component action is unsupported for this kind")
	}
	environmentID := component.Record.Desired.OwnerID
	head, found, err := service.blueprints.GetEnvironmentBlueprintHead(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !found {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "Environment has no desired Blueprint")
	}
	revision, found, err := service.blueprints.GetEnvironmentBlueprintRevision(ctx, environmentID, head.Record.RevisionID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !found {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment Blueprint head is missing its revision")
	}
	bundle := componentBlueprintBundle(revision.Record)
	if err := mutateComponentBlueprint(&bundle, component.Record.Desired.Kind, mutate); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.applier.ApplyComponentBlueprint(
		ctx, environmentID, component.Record.Desired.ID, bundle, idempotencyKey,
	)
}

func componentBlueprintBundle(revision etcd.EnvironmentBlueprintRevision) core.BlueprintBundle {
	files := make([]core.BlueprintFile, len(revision.Files))
	for index, file := range revision.Files {
		files[index] = core.BlueprintFile{Path: file.Path, Content: append([]byte(nil), file.Content...)}
	}
	return core.BlueprintBundle{
		RootPath: revision.RootPath, ComposeSources: append([]string(nil), revision.ComposeSources...),
		Files: files, Interpolation: cloneStringMap(revision.Interpolation),
	}
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func mutateComponentBlueprint(
	bundle *core.BlueprintBundle,
	kind core.ComponentKind,
	mutate func(*yaml.Node) error,
) error {
	rootIndex := -1
	for index := range bundle.Files {
		if bundle.Files[index].Path == bundle.RootPath {
			rootIndex = index
			break
		}
	}
	if rootIndex < 0 {
		return errs.New(errs.KindInternal, "Environment Blueprint root file is missing")
	}
	var document yaml.Node
	if err := yaml.Unmarshal(bundle.Files[rootIndex].Content, &document); err != nil ||
		len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return errs.New(errs.KindInternal, "Environment Blueprint root file is corrupt")
	}
	components := yamlMappingValue(document.Content[0], "x-gp-components")
	if components == nil || components.Kind != yaml.MappingNode {
		return errs.New(errs.KindStateConflict, "Environment Blueprint has no authored Components")
	}
	capability, err := environmentComponentCapability(kind)
	if err != nil {
		return err
	}
	matched := yamlMappingValue(components, string(capability))
	if matched == nil {
		return errs.New(errs.KindStateConflict, "Component is not authored by the current Blueprint")
	}
	implementation := yamlMappingValue(matched, "implementation")
	if implementation == nil || implementation.Value != string(kind) {
		return errs.New(errs.KindInternal, "Environment Blueprint Component implementation is corrupt")
	}
	if err := mutate(matched); err != nil {
		return err
	}
	content, err := yaml.Marshal(&document)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	bundle.Files[rootIndex].Content = content
	return nil
}

func environmentComponentCapability(kind core.ComponentKind) (core.ComponentCapability, error) {
	switch kind {
	case core.ComponentKindIngressCaddy:
		return core.ComponentCapabilityHTTPRouter, nil
	case core.ComponentKindEdgeCloudflare:
		return core.ComponentCapabilityEdgeTunnel, nil
	default:
		return "", errs.New(errs.KindStateConflict, "Component action is unsupported for this kind")
	}
}

func yamlMappingValue(mapping *yaml.Node, key string) *yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return mapping.Content[index+1]
		}
	}
	return nil
}

func replaceYAMLMappingValue(mapping *yaml.Node, key string, value *yaml.Node) error {
	if mapping == nil || mapping.Kind != yaml.MappingNode || value == nil {
		return errs.New(errs.KindInternal, "Component Blueprint mapping is invalid")
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			mapping.Content[index+1] = value
			return nil
		}
	}
	mapping.Content = append(mapping.Content, scalarNode("!!str", key), value)
	return nil
}

func removeYAMLMappingValue(mapping *yaml.Node, key string) {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			mapping.Content = append(mapping.Content[:index], mapping.Content[index+2:]...)
			return
		}
	}
}

func scalarNode(tag string, value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: value}
}
