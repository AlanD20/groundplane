package controller

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/danielgtaylor/huma/v2"
)

func componentConfigSchema(registry huma.Registry) *huma.Schema {
	reference := openAPISchema[apiTypes.ComponentConfig](registry, "ComponentConfig")
	schema := registry.SchemaFromRef(reference.Ref)
	if schema == nil {
		return reference
	}
	schema.Type = ""
	schema.AdditionalProperties = nil
	schema.Properties = nil
	schema.Required = nil
	schema.OneOf = []*huma.Schema{
		caddyComponentConfigSchema(),
		cloudflareTunnelComponentConfigResponseSchema(),
		coreDNSComponentConfigSchema(),
	}
	componentReference := openAPISchema[apiTypes.Component](registry, "Component")
	if componentSchema := registry.SchemaFromRef(componentReference.Ref); componentSchema != nil && componentSchema.Properties != nil {
		componentSchema.Properties["config"] = nullableComponentConfigSchema(reference.Ref)
	}
	return reference
}

func nullableComponentConfigSchema(ref string) *huma.Schema {
	return &huma.Schema{OneOf: []*huma.Schema{{Ref: ref}, {Type: "null"}}}
}

func componentConfigResponseSchema(registry huma.Registry) *huma.Schema {
	reference := openAPISchema[apiTypes.ComponentConfigResponse](registry, "ComponentConfigResponse")
	schema := registry.SchemaFromRef(reference.Ref)
	if schema == nil || schema.Properties == nil {
		return reference
	}
	configReference := openAPISchema[apiTypes.ComponentConfig](registry, "ComponentConfig")
	managedFileReference := openAPISchema[apiTypes.ManagedConfigFile](registry, "ManagedConfigFile")
	schema.Properties["config"] = nullableComponentConfigSchema(configReference.Ref)
	schema.Properties["managed_files"] = &huma.Schema{
		Type:  huma.TypeArray,
		Items: &huma.Schema{Ref: managedFileReference.Ref},
	}
	return reference
}

func componentConfigMutationRequestSchema(registry huma.Registry) *huma.Schema {
	reference := openAPISchema[apiTypes.ComponentConfigMutationRequest](registry, "ComponentConfigMutationRequest")
	schema := registry.SchemaFromRef(reference.Ref)
	if schema == nil {
		return reference
	}
	one := 1
	schema.AdditionalProperties = false
	schema.Required = []string{"config"}
	schema.MinProperties = &one
	schema.MaxProperties = &one
	schema.Properties["config"] = &huma.Schema{OneOf: []*huma.Schema{
		caddyComponentConfigSchema(),
		cloudflareTunnelComponentConfigSchema(),
		coreDNSComponentConfigSchema(),
	}}
	return reference
}

func coreDNSComponentConfigSchema() *huma.Schema {
	five := 5
	resolverList := &huma.Schema{
		Type:  huma.TypeArray,
		Items: &huma.Schema{Type: huma.TypeString},
	}
	forwarder := &huma.Schema{
		Type:                 huma.TypeObject,
		AdditionalProperties: false,
		Properties: map[string]*huma.Schema{
			"domain":    {Type: huma.TypeString},
			"resolvers": resolverList,
		},
		Required: []string{"domain", "resolvers"},
	}
	return &huma.Schema{
		Type:                 huma.TypeObject,
		AdditionalProperties: false,
		Properties: map[string]*huma.Schema{
			"corefile_template":  {Type: huma.TypeString},
			"upstream_auto":      {Type: huma.TypeBoolean},
			"upstream_resolvers": resolverList,
			"forwarders":         {Type: huma.TypeArray, Items: forwarder},
			"tailnet_delegation": {Type: huma.TypeBoolean},
		},
		Required:      []string{"corefile_template", "upstream_auto", "upstream_resolvers", "forwarders", "tailnet_delegation"},
		MinProperties: &five,
		MaxProperties: &five,
	}
}

func caddyComponentConfigSchema() *huma.Schema {
	one := 1
	two := 2
	return &huma.Schema{
		Type:                 huma.TypeObject,
		AdditionalProperties: false,
		Properties: map[string]*huma.Schema{
			"zone_id": {
				Type:    huma.TypeString,
				Pattern: "^net_[0-9A-HJKMNP-TV-Z]{26}$",
			},
			"caddyfile_template": {Type: huma.TypeString},
		},
		Required:      []string{"zone_id"},
		MinProperties: &one,
		MaxProperties: &two,
	}
}

func cloudflareTunnelComponentConfigSchema() *huma.Schema {
	one := 1
	return &huma.Schema{
		Type:                 huma.TypeObject,
		AdditionalProperties: false,
		Properties: map[string]*huma.Schema{
			"credential": {
				OneOf: []*huma.Schema{
					cloudflareTunnelExistingCredentialSchema(),
					cloudflareTunnelNewCredentialSchema(),
				},
			},
		},
		Required:      []string{"credential"},
		MinProperties: &one,
		MaxProperties: &one,
	}
}

func cloudflareTunnelComponentConfigResponseSchema() *huma.Schema {
	one := 1
	return &huma.Schema{
		Type:                 huma.TypeObject,
		AdditionalProperties: false,
		Properties: map[string]*huma.Schema{
			"secret_id": {
				Type:    huma.TypeString,
				Pattern: "^sec_[0-9A-HJKMNP-TV-Z]{26}$",
			},
		},
		Required:      []string{"secret_id"},
		MinProperties: &one,
		MaxProperties: &one,
	}
}

func cloudflareTunnelExistingCredentialSchema() *huma.Schema {
	two := 2
	return &huma.Schema{
		Type:                 huma.TypeObject,
		AdditionalProperties: false,
		Properties: map[string]*huma.Schema{
			"mode": {
				Type: huma.TypeString,
				Enum: []any{"existing"},
			},
			"secret_id": {
				Type:    huma.TypeString,
				Pattern: "^sec_[0-9A-HJKMNP-TV-Z]{26}$",
			},
		},
		Required:      []string{"mode", "secret_id"},
		MinProperties: &two,
		MaxProperties: &two,
	}
}

func cloudflareTunnelNewCredentialSchema() *huma.Schema {
	three := 3
	one := 1
	return &huma.Schema{
		Type:                 huma.TypeObject,
		AdditionalProperties: false,
		Properties: map[string]*huma.Schema{
			"mode": {
				Type: huma.TypeString,
				Enum: []any{"new"},
			},
			"secret_name": {Type: huma.TypeString},
			"token": {
				Type:      huma.TypeString,
				WriteOnly: true,
				MinLength: &one,
			},
		},
		Required:      []string{"mode", "secret_name", "token"},
		MinProperties: &three,
		MaxProperties: &three,
	}
}
