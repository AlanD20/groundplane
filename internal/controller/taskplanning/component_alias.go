package taskplanning

import (
	"slices"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/compose-spec/compose-go/v2/types"
)

func validateManagedNetworkAliases(project *types.Project, candidate types.ServiceConfig) error {
	for zone, attachment := range candidate.Networks {
		if attachment == nil {
			continue
		}
		for _, services := range []types.Services{project.Services, project.DisabledServices} {
			for name, other := range services {
				network, present := other.Networks[zone]
				if !present {
					continue
				}
				for _, alias := range attachment.Aliases {
					if alias == name || network != nil && slices.Contains(network.Aliases, alias) {
						return errs.New(
							errs.KindNameConflict,
							"router network alias conflicts with another Service on the selected Zone",
						)
					}
				}
			}
		}
	}
	return nil
}
