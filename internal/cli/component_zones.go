package cli

import (
	"fmt"
	"strings"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

type componentZoneCreation struct {
	name     string
	subnet   string
	internal bool
}

func parseComponentZoneCreations(ordinary, internal []string) ([]componentZoneCreation, error) {
	creations := make([]componentZoneCreation, 0, len(ordinary)+len(internal))
	appendValues := func(values []string, isInternal bool, flag string) error {
		for _, value := range values {
			name, subnet, ok := strings.Cut(value, "=")
			if !ok || name == "" || subnet == "" {
				return fmt.Errorf("%s must use NAME=CIDR", flag)
			}
			creations = append(creations, componentZoneCreation{name: name, subnet: subnet, internal: isInternal})
		}
		return nil
	}
	if err := appendValues(ordinary, false, "--create-zone"); err != nil {
		return nil, err
	}
	if err := appendValues(internal, true, "--create-internal-zone"); err != nil {
		return nil, err
	}
	return creations, nil
}

func resolveComponentZoneIDs(cmd *cobra.Command, environmentID string, arguments []string) ([]string, error) {
	if len(arguments) == 0 {
		return nil, nil
	}
	app := fromContext(cmd)
	if app.Scope.AsID {
		return append([]string(nil), arguments...), nil
	}
	zonesByName := make(map[string]string)
	cursor := ""
	for {
		page, err := app.Client.ListZones(cmd.Context(), environmentID, 200, cursor)
		if err != nil {
			return nil, err
		}
		for _, zone := range page.Items {
			if zone.OwnerKind == apiTypes.ZoneOwnerEnvironment && zone.EnvironmentID == environmentID {
				zonesByName[zone.Name] = zone.ID
			}
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	result := make([]string, len(arguments))
	for index, argument := range arguments {
		id, ok := zonesByName[argument]
		if !ok {
			return nil, errs.Newf(
				errs.KindZoneNotFound,
				"zone name %q was not found in the Component Environment",
				argument,
			)
		}
		result[index] = id
	}
	return result, nil
}

func createComponentZones(
	cmd *cobra.Command,
	environmentID string,
	creations []componentZoneCreation,
) ([]apiTypes.Zone, error) {
	created := make([]apiTypes.Zone, 0, len(creations))
	for _, creation := range creations {
		zone, err := fromContext(cmd).Client.CreateZone(cmd.Context(), apiTypes.ZoneCreate{
			EnvironmentID: environmentID,
			Name:          creation.name,
			Subnet:        creation.subnet,
			Internal:      creation.internal,
		})
		if err != nil {
			return created, err
		}
		created = append(created, zone)
	}
	return created, nil
}

func returnWithRetainedComponentZones(cmd *cobra.Command, created []apiTypes.Zone, operationErr error) error {
	for _, zone := range created {
		if _, err := fmt.Fprintf(
			cmd.ErrOrStderr(),
			"warning: Zone %s (%s) was created and remains available; the Component operation did not roll it back\n",
			zone.Name,
			zone.ID,
		); err != nil {
			return fmt.Errorf("%w; reporting retained Zone: %v", operationErr, err)
		}
	}
	return operationErr
}
