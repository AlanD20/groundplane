package component

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

type mutationRepository interface {
	GetComponent(context.Context, string) (etcdstore.Versioned[componentrecord.Record], error)
}

type blueprintApplier interface {
	GetBlueprint(context.Context, string) (apiTypes.EnvironmentBlueprintDocument, error)
	TaskAcceptanceIdentity(context.Context, string) (workexecution.Scope, string, time.Duration, error)
	PrepareAcceptedComponentBlueprint(
		context.Context,
		workqueue.TaskSnapshot,
		core.BlueprintBundle,
		string,
		idempotencyrecord.IdempotencyLocator,
		idempotencyrecord.ProtectedIntentRecord,
	) (taskdependencies.Prepared, error)
}

type credentialReferenceResolver interface {
	ResolveCredentialReference(context.Context, string, OpaqueSecretReferenceInput, string) (string, error)
}

type platformConfigMutator interface {
	EnablePlatformComponent(
		context.Context,
		string,
		apiTypes.TaskRunAfterRequest,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
	DisablePlatformComponent(
		context.Context,
		string,
		apiTypes.TaskRunAfterRequest,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
	UpdatePlatformComponent(
		context.Context,
		string,
		apiTypes.TaskRunAfterRequest,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
	ReplacePlatformComponentConfig(
		context.Context,
		string,
		apiTypes.ComponentConfigMutationRequest,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
}

type MutationService struct {
	acceptedActions *acceptedaction.Submitter
	components      mutationRepository
	applier         blueprintApplier
	credentials     credentialReferenceResolver
	platform        platformConfigMutator
}

func NewMutationService(
	components mutationRepository,
	applier blueprintApplier,
	credentials credentialReferenceResolver,
	platform platformConfigMutator,
	acceptedActions *acceptedaction.Submitter,
) (*MutationService, error) {
	if components == nil || applier == nil || credentials == nil || platform == nil || acceptedActions == nil {
		return nil, errs.New(errs.KindInternal, "Component mutation dependencies are not configured")
	}
	return &MutationService{
		acceptedActions: acceptedActions, components: components,
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
		return service.platform.EnablePlatformComponent(
			ctx,
			componentID,
			apiTypes.TaskRunAfterRequest{After: request.After},
			idempotencyKey,
		)
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
	return service.submitComponent(ctx, current.Record, "enable", config, request.After, idempotencyKey)
}

func (service *MutationService) DisableComponent(
	ctx context.Context,
	componentID string,
	input apiTypes.TaskRunAfterRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	current, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if current.Record.Desired.Owner == core.ComponentOwnerPlatform {
		return service.platform.DisablePlatformComponent(ctx, componentID, input, idempotencyKey)
	}
	return service.submitComponent(ctx, current.Record, "disable", nil, input.After, idempotencyKey)
}

func (service *MutationService) UpdateComponent(
	ctx context.Context,
	componentID string,
	input apiTypes.TaskRunAfterRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	current, err := service.components.GetComponent(ctx, componentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if current.Record.Desired.Owner == core.ComponentOwnerPlatform {
		return service.platform.UpdatePlatformComponent(ctx, componentID, input, idempotencyKey)
	}
	return service.submitComponent(ctx, current.Record, "update", nil, input.After, idempotencyKey)
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
	return service.submitComponent(ctx, current.Record, "configure", &publicConfig, request.After, idempotencyKey)
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
	capability, err := environmentComponentCapability(kind)
	if err != nil {
		return err
	}
	components := yamlMappingValue(document.Content[0], "x-gp-components")
	if components == nil {
		components = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		if err := replaceYAMLMappingValue(document.Content[0], "x-gp-components", components); err != nil {
			return err
		}
	}
	if components.Kind != yaml.MappingNode {
		return errs.New(errs.KindInternal, "Environment Blueprint Components mapping is corrupt")
	}
	matched := yamlMappingValue(components, string(capability))
	if matched == nil {
		matched = &yaml.Node{}
		if err := matched.Encode(core.ComponentSpec{Implementation: kind}); err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if err := replaceYAMLMappingValue(components, string(capability), matched); err != nil {
			return err
		}
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
	if capability, ok := kind.EnvironmentCapability(); ok {
		return capability, nil
	}
	return "", errs.New(errs.KindStateConflict, "Component action is unsupported for this kind")
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
