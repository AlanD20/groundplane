package blueprint

import (
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
)

// Display labels are captured with the graph, never resolved from live resources
// when reading history. They do not participate in effect permission or identity.
func captureAuthoredWorkNames(graph *blueprintunits.CapturedGraph, sources ...projectionrecord.EnvironmentOwnedIdentities) error {
	names := make(map[string]string)
	for _, source := range sources {
		if source.EnvironmentID == "" {
			continue
		}
		targets, err := selectAuthoredOwnedUnitTargets(source)
		if err != nil {
			return err
		}
		for _, target := range targets {
			name := target.Name
			if len(name) > 256 {
				end := 253
				for !utf8.RuneStart(name[end]) {
					end--
				}
				name = name[:end] + "…"
			}
			names[target.Target.ID] = name
		}
	}
	for index := range graph.Items {
		graph.Items[index].TargetName = names[graph.Items[index].TargetID]
	}
	return nil
}
