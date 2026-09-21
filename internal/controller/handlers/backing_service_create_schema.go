package handlers

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/danielgtaylor/huma/v2"
)

// Keep shared typed fields while enforcing the adapter's complete input choice
// in both OpenAPI and HTTP validation, before the mutation service is called.
func backingServiceCreateSchema(registry huma.Registry) {
	reference := openAPISchema[apiTypes.BackingServiceCreate](registry, "BackingServiceCreate")
	schema := registry.SchemaFromRef(reference.Ref)
	one := 1
	present := func(name string) *huma.Schema {
		return &huma.Schema{
			Type: huma.TypeObject, Properties: map[string]*huma.Schema{name: {}}, Required: []string{name},
		}
	}
	schema.OneOf = []*huma.Schema{
		{Type: huma.TypeObject, Properties: map[string]*huma.Schema{
			"adapter":        {Type: huma.TypeString, Enum: []any{"valkey:9"}},
			"authentication": schema.Properties["authentication"],
		}, Required: []string{"adapter", "authentication"}, Not: &huma.Schema{
			AnyOf: []*huma.Schema{
				present("image"),
				present("hooks"),
			},
		}},
		{Type: huma.TypeObject, Properties: map[string]*huma.Schema{
			"adapter": {Type: huma.TypeString, Enum: []any{"postgres:16"}},
		}, Required: []string{"adapter"}, Not: &huma.Schema{AnyOf: []*huma.Schema{
			present("authentication"),
			present("image"),
			present("hooks"),
		}}},
		{Type: huma.TypeObject, Properties: map[string]*huma.Schema{
			"adapter": {Type: huma.TypeString, Enum: []any{"custom"}},
			"image":   {Type: huma.TypeString, MinLength: &one},
		}, Required: []string{"adapter", "image"}, Not: present("authentication")},
	}
	schema.PrecomputeMessages()
}
