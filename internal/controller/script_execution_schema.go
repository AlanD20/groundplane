package controller

import (
	"maps"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/danielgtaylor/huma/v2"
)

func scriptExecutionSchema(registry huma.Registry) {
	reference := openAPISchema[apiTypes.ScriptExecution](registry, "ScriptExecution")
	schema := registry.SchemaFromRef(reference.Ref)
	if schema == nil {
		return
	}
	// Optional lists are omitted or actual arrays, never nullable reset aliases.
	maximumVolumes, maximumEntries := 32, 64
	grant := openAPISchema[apiTypes.ScriptVolumeGrant](registry, "ScriptVolumeGrant")
	schema.Properties["volumes"] = &huma.Schema{Type: huma.TypeArray, Items: grant, MaxItems: &maximumVolumes}
	schema.Properties["entry_ids"] = &huma.Schema{Type: huma.TypeArray,
		Items: &huma.Schema{Type: huma.TypeString}, MaxItems: &maximumEntries}
	// Keep the typed fields for generated Go clients, while oneOf closes the
	// two complete choices. No client needs opaque JSON union conversions.
	explicit := &huma.Schema{Type: huma.TypeObject, AdditionalProperties: false,
		Properties: maps.Clone(schema.Properties), Required: []string{"mode", "image", "user"}}
	explicit.Properties["mode"] = &huma.Schema{Type: huma.TypeString, Enum: []any{"explicit"}}
	schema.OneOf = []*huma.Schema{
		{Type: huma.TypeObject, AdditionalProperties: false,
			Properties: map[string]*huma.Schema{"mode": {Type: huma.TypeString, Enum: []any{"inherited"}}},
			Required:   []string{"mode"}},
		explicit,
	}
	// Huma caches object property/required names when the original type is
	// registered. Rebuild them after replacing it with the closed union.
	schema.PrecomputeMessages()
	for _, parent := range []*huma.Schema{
		openAPISchema[apiTypes.Script](registry, "Script"),
		openAPISchema[apiTypes.ScriptCreate](registry, "ScriptCreate"),
		openAPISchema[apiTypes.ScriptEdit](registry, "ScriptEdit"),
	} {
		if parentSchema := registry.SchemaFromRef(parent.Ref); parentSchema != nil {
			parentSchema.Properties["execution"] = &huma.Schema{Ref: reference.Ref}
			parentSchema.PrecomputeMessages()
		}
	}
}
