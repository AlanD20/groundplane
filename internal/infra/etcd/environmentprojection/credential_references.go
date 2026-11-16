package environmentprojection

import "slices"

// ComponentSecretIDs are stable references authored in the accepted input.
// They must remain protected even before a Component has runtime metadata.
func (value EnvironmentDesiredInput) ComponentSecretIDs() []string {
	var result []string
	for _, component := range value.Input.Components {
		if id := component.Settings.SecretID; id != "" {
			result = append(result, id)
		}
	}
	slices.Sort(result)
	return slices.Compact(result)
}
