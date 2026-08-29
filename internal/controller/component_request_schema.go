package controller

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/danielgtaylor/huma/v2"
)

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
	four := 4
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
			"upstream_auto":      {Type: huma.TypeBoolean},
			"upstream_resolvers": resolverList,
			"forwarders":         {Type: huma.TypeArray, Items: forwarder},
			"tailnet_delegation": {Type: huma.TypeBoolean},
		},
		Required:      []string{"upstream_auto", "upstream_resolvers", "forwarders", "tailnet_delegation"},
		MinProperties: &four,
		MaxProperties: &four,
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
			},
		},
		Required:      []string{"mode", "secret_name", "token"},
		MinProperties: &three,
		MaxProperties: &three,
	}
}
