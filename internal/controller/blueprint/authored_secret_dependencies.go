package blueprint

import "context"

// Secret references remain authored keys in desired state. Unit dependencies
// use their resolved stable identities, just as Entry value generation does.
func (service *Service) resolveAuthoredSecretIDs(
	ctx context.Context, input authoredParentInput,
) (map[string]string, error) {
	resolved := make(map[string]string)
	for _, name := range sortedAuthoredMapKeys(input.desired.Input.Entries) {
		reference := input.desired.Input.Entries[name].Source.SecretRef
		if reference == "" {
			continue
		}
		if _, found := resolved[reference]; found {
			continue
		}
		id, err := service.entryGeneration.ResolveSecretID(ctx, input.owner.ProjectID, reference)
		if err != nil {
			return nil, err
		}
		resolved[reference] = id
	}
	return resolved, nil
}
