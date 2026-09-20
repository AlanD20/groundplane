package component

import (
	"context"
	"encoding/json"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/core"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

type mutationRepository interface {
	GetComponent(context.Context, string) (etcdstore.Versioned[componentrecord.Record], error)
}

type blueprintApplier interface {
	GetBlueprint(context.Context, string) (apiTypes.EnvironmentBlueprintDocument, error)
	ApplyComponentBlueprint(
		context.Context,
		string,
		string,
		core.BlueprintBundle,
		string,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
}

type credentialReferenceResolver interface {
	ResolveCredentialReference(context.Context, string, OpaqueSecretReferenceInput, string) (string, error)
}

type platformConfigMutator interface {
	EnablePlatformComponent(context.Context, string, string) (idempotencyrecord.IdempotencyResponse, error)
	DisablePlatformComponent(context.Context, string, string) (idempotencyrecord.IdempotencyResponse, error)
	UpdatePlatformComponent(context.Context, string, string) (idempotencyrecord.IdempotencyResponse, error)
	ReplacePlatformComponentConfig(
		context.Context,
		string,
		apiTypes.ComponentConfigMutationRequest,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
}

type MutationService struct {
	components  mutationRepository
	applier     blueprintApplier
	credentials credentialReferenceResolver
	platform    platformConfigMutator
}

func NewMutationService(
	components mutationRepository,
	applier blueprintApplier,
	credentials credentialReferenceResolver,
	platform platformConfigMutator,
) (*MutationService, error) {
	if components == nil || applier == nil || credentials == nil || platform == nil {
		return nil, errs.New(errs.KindInternal, "Component mutation dependencies are not configured")
	}
	return &MutationService{
		components:  components,
		applier:     applier,
		credentials: credentials,
		platform:    platform,
	}, nil
}

func (service *MutationService) EnableComponent(
	ctx context.Context,
	componentID string,
	request apiTypes.ComponentEnableRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	current, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if current.Record.Desired.Owner == core.ComponentOwnerPlatform {
		if request.Config != nil {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindValidationFailed,
				"Platform Component enable does not accept config",
			)
		}
		return service.platform.EnablePlatformComponent(ctx, componentID, idempotencyKey)
	}
	var config *apiTypes.ComponentConfig
	if request.Config != nil {
		prepared, err := service.environmentComponentConfig(
			ctx,
			current.Record.Desired,
			*request.Config,
			idempotencyKey,
		)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		config = &prepared
	}
	return service.mutateComponent(ctx, componentID, idempotencyKey, func(spec *yaml.Node) error {
		if config != nil {
			if err := applyEnvironmentComponentConfig(spec, *config); err != nil {
				return err
			}
		}
		return replaceYAMLMappingValue(spec, "enabled", scalarNode("!!bool", strconv.FormatBool(true)))
	})
}

func (service *MutationService) DisableComponent(
	ctx context.Context,
	componentID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	current, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
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
) (idempotencyrecord.IdempotencyResponse, error) {
	current, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
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
) (idempotencyrecord.IdempotencyResponse, error) {
	if err := request.Config.Validate(); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, err.Error())
	}
	current, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if current.Record.Desired.Owner == core.ComponentOwnerPlatform {
		return service.platform.ReplacePlatformComponentConfig(ctx, componentID, request, idempotencyKey)
	}
	publicConfig, err := service.environmentComponentConfig(ctx, current.Record.Desired, request.Config, idempotencyKey)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	response, err := service.mutateComponent(ctx, componentID, idempotencyKey, func(spec *yaml.Node) error {
		return applyEnvironmentComponentConfig(spec, publicConfig)
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	var accepted apiTypes.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil || accepted.TaskID == "" {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Component Blueprint Task response is invalid")
	}
	body, err := json.Marshal(apiTypes.ComponentConfigMutationResult{
		Resource: publicConfig, ReconcileTaskID: &accepted.TaskID,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	return idempotencyrecord.IdempotencyResponse{Status: http.StatusOK, ContentKind: "application/json", Body: body}, nil
}

func (service *MutationService) mutateComponent(
	ctx context.Context,
	componentID string,
	idempotencyKey string,
	mutate func(*yaml.Node) error,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Component mutation context is required")
	}
	component, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if component.Record.Desired.Owner != core.ComponentOwnerEnvironment {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Platform Component actions require their dedicated lifecycle API",
		)
	}
	if component.Record.Desired.Kind != core.ComponentKindIngressCaddy &&
		component.Record.Desired.Kind != core.ComponentKindEdgeCloudflare {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Component action is unsupported for this kind",
		)
	}
	environmentID := component.Record.Desired.OwnerID
	document, err := service.applier.GetBlueprint(ctx, environmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if document.EnvironmentID != environmentID || document.Revision == "" {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Component Blueprint authoring identity is invalid",
		)
	}
	bundle := core.BlueprintBundle{
		RootPath: "groundplane.yaml", ComposeSources: []string{"groundplane.yaml"},
		Files: []core.BlueprintFile{{Path: "groundplane.yaml", Content: []byte(document.Document)}},
	}
	if err := mutateComponentBlueprint(&bundle, component.Record.Desired.Kind, mutate); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return service.applier.ApplyComponentBlueprint(
		ctx, environmentID, component.Record.Desired.ID, bundle, document.Revision, idempotencyKey,
	)
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
