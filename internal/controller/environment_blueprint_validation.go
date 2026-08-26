package controller

import (
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ValidateEnvironmentBlueprintAvailability rejects valid authored contracts
// whose durable Controller behavior is not implemented yet.
func ValidateEnvironmentBlueprintAvailability(parsed blueprintparser.Result) error {
	extensions := parsed.Extensions
	if parsed.Project == nil || len(extensions.Requires) != 0 || len(extensions.Attachments) != 0 ||
		len(extensions.Entries) != 0 || extensions.Backup != nil ||
		len(extensions.ReleaseGroups) != 0 || len(parsed.Project.Configs) != 0 ||
		len(parsed.Project.Secrets) != 0 {
		return errs.New(errs.KindValidationFailed, "Blueprint uses a desired-state contract that is not available yet")
	}
	for _, network := range parsed.Project.Networks {
		if network.External || len(network.Extensions) != 0 {
			return errs.New(errs.KindValidationFailed, "Blueprint external network ownership is not available yet")
		}
	}
	for _, volume := range parsed.Project.Volumes {
		if volume.External || (volume.Driver != "" && volume.Driver != "local") || len(volume.DriverOpts) != 0 ||
			len(volume.Extensions) != 0 {
			return errs.New(errs.KindValidationFailed, "Blueprint volume runtime is not managed by Groundplane")
		}
	}
	return nil
}
