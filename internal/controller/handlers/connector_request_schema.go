package handlers

import (
	"reflect"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/danielgtaylor/huma/v2"
)

func openAPISchema[T any](registry huma.Registry, name string) *huma.Schema {
	return registry.Schema(reflect.TypeFor[T](), true, name)
}

func connectorCreateRequestSchema(registry huma.Registry) *huma.Schema {
	reference := registry.Schema(
		reflect.TypeFor[apiTypes.ConnectorCreateRequest](),
		true,
		"ConnectorCreateRequest",
	)
	schema := registry.SchemaFromRef(reference.Ref)
	if schema == nil {
		return reference
	}

	if pathStyle := schema.Properties["path_style"]; pathStyle != nil {
		pathStyle.Nullable = false
	}

	credentials := schema.Properties["credentials"]
	if credentials == nil {
		return reference
	}

	credentialReference := registry.Schema(
		reflect.TypeFor[apiTypes.ConnectorCredentialInput](),
		true,
		"ConnectorCredentialInput",
	)
	if credentialSchema := registry.SchemaFromRef(credentialReference.Ref); credentialSchema != nil {
		credentialSchema.OneOf = []*huma.Schema{
			connectorCredentialVariant("secret_ref"),
			connectorCredentialVariant("value"),
		}
	}

	two := 2
	credentials.AdditionalProperties = false
	credentials.Properties = map[string]*huma.Schema{
		"access_key": credentialReference,
		"secret_key": credentialReference,
	}
	credentials.Required = []string{"access_key", "secret_key"}
	credentials.MinProperties = &two
	credentials.MaxProperties = &two

	return reference
}

func connectorCredentialVariant(field string) *huma.Schema {
	return &huma.Schema{
		Type:                 huma.TypeObject,
		AdditionalProperties: false,
		Properties: map[string]*huma.Schema{
			field: {Type: huma.TypeString},
		},
		Required: []string{field},
	}
}
