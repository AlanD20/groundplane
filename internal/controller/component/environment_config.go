package component

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"

	"github.com/AlanD20/groundplane/internal/core"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

func (service *MutationService) environmentComponentConfig(
	ctx context.Context,
	desired componentrecord.DesiredRecord,
	input apiTypes.ComponentConfigMutationInput,
	idempotencyKey string,
) (apiTypes.ComponentConfig, error) {
	if err := input.Validate(); err != nil {
		return apiTypes.ComponentConfig{}, errs.New(errs.KindValidationFailed, err.Error())
	}
	switch desired.Kind {
	case core.ComponentKindIngressCaddy:
		if input.Caddy != nil {
			return apiTypes.ComponentConfig{Caddy: &apiTypes.CaddyComponentConfig{
				ZoneIDs:           append([]string(nil), input.Caddy.ZoneIDs...),
				CaddyfileTemplate: input.Caddy.CaddyfileTemplate, Alias: input.Caddy.Alias,
			}}, nil
		}
	case core.ComponentKindEdgeCloudflare:
		if input.CloudflareTunnel != nil {
			credential := input.CloudflareTunnel.Credential
			secretID, err := service.credentials.ResolveCredentialReference(ctx, desired.OwnerID,
				OpaqueSecretReferenceInput{Mode: credential.Mode, SecretID: credential.SecretID,
					Name: credential.SecretName, Value: credential.Token}, idempotencyKey)
			if err != nil {
				return apiTypes.ComponentConfig{}, err
			}
			return apiTypes.ComponentConfig{CloudflareTunnel: &apiTypes.CloudflareTunnelComponentConfig{
				ZoneIDs: append([]string(nil), input.CloudflareTunnel.ZoneIDs...), SecretID: secretID,
			}}, nil
		}
	}
	return apiTypes.ComponentConfig{}, errs.New(
		errs.KindValidationFailed,
		"Component config does not match its implementation",
	)
}

func applyEnvironmentComponentConfig(spec *yaml.Node, config apiTypes.ComponentConfig) error {
	settings := core.ComponentCapabilitySettings{}
	implementation := core.ComponentImplementationConfig{}
	switch {
	case config.Caddy != nil:
		settings.ZoneIDs = append([]string(nil), config.Caddy.ZoneIDs...)
		settings.Alias = config.Caddy.Alias
		implementation.CaddyfileTemplate = config.Caddy.CaddyfileTemplate
	case config.CloudflareTunnel != nil:
		settings.ZoneIDs = append([]string(nil), config.CloudflareTunnel.ZoneIDs...)
		settings.SecretID = config.CloudflareTunnel.SecretID
	default:
		return errs.New(errs.KindValidationFailed, "Environment Component config is required")
	}
	var node yaml.Node
	if err := node.Encode(settings); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := replaceYAMLMappingValue(spec, "settings", &node); err != nil {
		return err
	}
	if implementation.CaddyfileTemplate == "" {
		removeYAMLMappingValue(spec, "implementation_config")
		return nil
	}
	var implementationNode yaml.Node
	if err := implementationNode.Encode(implementation); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return replaceYAMLMappingValue(spec, "implementation_config", &implementationNode)
}
