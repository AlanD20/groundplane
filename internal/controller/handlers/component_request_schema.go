package handlers

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
		envoyComponentConfigSchema(),
		cloudflareTunnelComponentConfigResponseSchema(),
		coreDNSComponentConfigSchema(),
	}
	componentReference := openAPISchema[apiTypes.Component](registry, "Component")
	if componentSchema := registry.SchemaFromRef(componentReference.Ref); componentSchema != nil &&
		componentSchema.Properties != nil {
		componentSchema.Properties["config"] = nullableComponentConfigSchema(reference.Ref)
		componentSchema.PrecomputeMessages()
	}
	schema.PrecomputeMessages()
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
	schema.PrecomputeMessages()
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
		envoyComponentConfigSchema(),
		cloudflareTunnelComponentConfigSchema(),
		coreDNSComponentConfigSchema(),
	}}
	schema.PrecomputeMessages()
	return reference
}

func componentEnableRequestSchema(registry huma.Registry) *huma.Schema {
	reference := openAPISchema[apiTypes.ComponentEnableRequest](registry, "ComponentEnableRequest")
	schema := registry.SchemaFromRef(reference.Ref)
	if schema == nil {
		return reference
	}
	schema.AdditionalProperties = false
	schema.Required = nil
	schema.Properties = map[string]*huma.Schema{"config": {OneOf: []*huma.Schema{
		caddyComponentConfigSchema(), envoyComponentConfigSchema(), cloudflareTunnelComponentConfigSchema(),
	}}}
	schema.PrecomputeMessages()
	return reference
}

func coreDNSComponentConfigSchema() *huma.Schema {
	six := 6
	seven := 7
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
			"corefile_template": {Type: huma.TypeString},
			"implementation":    componentImplementationSchema("coredns"),
			"records": {
				Type:  huma.TypeArray,
				Items: &huma.Schema{OneOf: []*huma.Schema{dnsAddressRecordSchema(), dnsServiceRecordSchema()}},
			},
			"upstream_auto":      {Type: huma.TypeBoolean},
			"upstream_resolvers": resolverList,
			"forwarders":         {Type: huma.TypeArray, Items: forwarder},
			"tailnet_delegation": {Type: huma.TypeBoolean},
		},
		Required: []string{
			"implementation",
			"corefile_template",
			"upstream_auto",
			"upstream_resolvers",
			"forwarders",
			"tailnet_delegation",
		},
		MinProperties: &six,
		MaxProperties: &seven,
	}
}

func caddyComponentConfigSchema() *huma.Schema {
	two := 2
	four := 4
	return &huma.Schema{
		Type:                 huma.TypeObject,
		AdditionalProperties: false,
		Properties: map[string]*huma.Schema{
			"implementation":     componentImplementationSchema("caddy"),
			"zone_ids":           componentZoneIDsSchema(),
			"caddyfile_template": {Type: huma.TypeString},
			"alias":              {Type: huma.TypeString, Pattern: "^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)?$"},
		},
		Required:      []string{"implementation", "zone_ids"},
		MinProperties: &two,
		MaxProperties: &four,
	}
}

func envoyComponentConfigSchema() *huma.Schema {
	two := 2
	three := 3
	return &huma.Schema{
		Type: huma.TypeObject, AdditionalProperties: false,
		Properties: map[string]*huma.Schema{
			"implementation": componentImplementationSchema("envoy"),
			"zone_ids":       componentZoneIDsSchema(),
			"alias":          {Type: huma.TypeString, Pattern: "^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)?$"},
		},
		Required: []string{"implementation", "zone_ids"}, MinProperties: &two, MaxProperties: &three,
	}
}

func componentImplementationSchema(implementation string) *huma.Schema {
	return &huma.Schema{Type: huma.TypeString, Enum: []any{implementation}}
}

func cloudflareTunnelComponentConfigSchema() *huma.Schema {
	three := 3
	return &huma.Schema{
		Type:                 huma.TypeObject,
		AdditionalProperties: false,
		Properties: map[string]*huma.Schema{
			"implementation": componentImplementationSchema("cloudflare-tunnel"),
			"zone_ids":       componentZoneIDsSchema(),
			"credential": {
				OneOf: []*huma.Schema{
					cloudflareTunnelExistingCredentialSchema(),
					cloudflareTunnelNewCredentialSchema(),
				},
			},
		},
		Required:      []string{"implementation", "zone_ids", "credential"},
		MinProperties: &three,
		MaxProperties: &three,
	}
}

func cloudflareTunnelComponentConfigResponseSchema() *huma.Schema {
	three := 3
	return &huma.Schema{
		Type:                 huma.TypeObject,
		AdditionalProperties: false,
		Properties: map[string]*huma.Schema{
			"implementation": componentImplementationSchema("cloudflare-tunnel"),
			"zone_ids":       componentZoneIDsSchema(),
			"secret_id": {
				Type:    huma.TypeString,
				Pattern: "^sec_[0-9A-HJKMNP-TV-Z]{26}$",
			},
		},
		Required:      []string{"implementation", "zone_ids", "secret_id"},
		MinProperties: &three,
		MaxProperties: &three,
	}
}

func componentZoneIDsSchema() *huma.Schema {
	one := 1
	return &huma.Schema{
		Type: huma.TypeArray, MinItems: &one, UniqueItems: true,
		Description: "Ordered selected Environment Zone IDs; the first router Zone is primary.",
		Items:       &huma.Schema{Type: huma.TypeString, Pattern: "^net_[0-9A-HJKMNP-TV-Z]{26}$"},
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
