package taskplanning

import (
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func BackingHookConfigurationFromAPI(source *apiTypes.BackingHookConfiguration) *backinghook.Configuration {
	if source == nil {
		return nil
	}
	result := &backinghook.Configuration{
		Attach:     backingHookDefinitionFromAPI(source.Attach),
		Detach:     backingHookDefinitionFromAPI(source.Detach),
		BeforeStop: backingHookDefinitionFromAPI(source.BeforeStop),
		AfterStart: backingHookDefinitionFromAPI(source.AfterStart),
		Facts:      make([]backinghook.FactDefinition, len(source.Facts)),
		Inputs:     make([]backinghook.InputDefinition, len(source.Inputs)),
	}
	for index, fact := range source.Facts {
		result.Facts[index] = backinghook.FactDefinition{Key: fact.Key, Secret: fact.Secret}
	}
	for index, input := range source.Inputs {
		result.Inputs[index] = backinghook.InputDefinition{
			Key: input.Key, Value: cloneHookLiteral(input.Value), SecretRef: input.SecretRef,
			Generate: backinghook.InputGeneration(input.Generate),
		}
	}
	if result.Attach == nil && result.Detach == nil && result.BeforeStop == nil && result.AfterStart == nil &&
		len(result.Facts) == 0 && len(result.Inputs) == 0 {
		return nil
	}
	return result
}

func backingHookDefinitionFromAPI(source *apiTypes.BackingHookDefinition) *backinghook.Definition {
	if source == nil {
		return nil
	}
	return &backinghook.Definition{
		Command: append([]string(nil), source.Command...), TimeoutSeconds: source.TimeoutSeconds,
	}
}

func BackingHookConfigurationToAPI(source *backinghook.Configuration) *apiTypes.BackingHookConfiguration {
	if source == nil {
		return nil
	}
	result := &apiTypes.BackingHookConfiguration{
		Attach:     backingHookDefinitionToAPI(source.Attach),
		Detach:     backingHookDefinitionToAPI(source.Detach),
		BeforeStop: backingHookDefinitionToAPI(source.BeforeStop),
		AfterStart: backingHookDefinitionToAPI(source.AfterStart),
		Facts:      make([]apiTypes.BackingHookFactDefinition, len(source.Facts)),
		Inputs:     make([]apiTypes.BackingHookInput, len(source.Inputs)),
	}
	for index, fact := range source.Facts {
		result.Facts[index] = apiTypes.BackingHookFactDefinition{Key: fact.Key, Secret: fact.Secret}
	}
	for index, input := range source.Inputs {
		result.Inputs[index] = apiTypes.BackingHookInput{
			Key: input.Key, Value: cloneHookLiteral(input.Value), SecretRef: input.SecretRef,
			Generate: string(input.Generate),
		}
	}
	return result
}

func backingHookDefinitionToAPI(source *backinghook.Definition) *apiTypes.BackingHookDefinition {
	if source == nil {
		return nil
	}
	return &apiTypes.BackingHookDefinition{
		Command: append([]string(nil), source.Command...), TimeoutSeconds: source.TimeoutSeconds,
	}
}

func cloneHookLiteral(source *string) *string {
	if source == nil {
		return nil
	}
	value := *source
	return &value
}
