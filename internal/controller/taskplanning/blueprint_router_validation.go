package taskplanning

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/componentrender"
)

// ValidateBlueprintRouterConfiguration checks configuration that is already
// decidable from authored Routes, before any resource or runtime is published.
func ValidateBlueprintRouterConfiguration(parsed blueprintparser.Result,
	catalog []componentrender.EnvironmentComponentRegistration,
) error {
	keys := make([]string, 0, len(parsed.Extensions.Components))
	for key := range parsed.Extensions.Components {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		spec := parsed.Extensions.Components[key]
		if !spec.Enabled {
			continue
		}
		for _, registration := range catalog {
			if registration.Kind == spec.Implementation && registration.ValidateAuthoredRoutes != nil {
				if err := registration.ValidateAuthoredRoutes(spec, parsed.Extensions.Routes); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
