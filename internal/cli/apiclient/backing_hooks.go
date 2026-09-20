package apiclient

import (
	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func backingHooksToGenerated(input *apiTypes.BackingHookConfiguration) *generated.BackingHookConfiguration {
	if input == nil {
		return nil
	}
	result := &generated.BackingHookConfiguration{
		Attach: backingHookToGenerated(input.Attach), Detach: backingHookToGenerated(input.Detach),
		BeforeStop: backingHookToGenerated(input.BeforeStop), AfterStart: backingHookToGenerated(input.AfterStart),
	}
	if input.Facts != nil {
		facts := make([]generated.BackingHookFactDefinition, len(input.Facts))
		for index, fact := range input.Facts {
			facts[index] = generated.BackingHookFactDefinition{Key: fact.Key, Secret: fact.Secret}
		}
		result.Facts = &facts
	}
	if input.Inputs != nil {
		inputs := make([]generated.BackingHookInput, len(input.Inputs))
		for index, value := range input.Inputs {
			inputs[index] = generated.BackingHookInput{Key: value.Key, Value: value.Value}
			if value.SecretRef != "" {
				inputs[index].SecretRef = &value.SecretRef
			}
			if value.Generate != "" {
				generation := generated.BackingHookInputGenerate(value.Generate)
				inputs[index].Generate = &generation
			}
		}
		result.Inputs = &inputs
	}
	return result
}

func backingHookToGenerated(input *apiTypes.BackingHookDefinition) *generated.BackingHookDefinition {
	if input == nil {
		return nil
	}
	return &generated.BackingHookDefinition{Command: &input.Command, TimeoutSeconds: int32(input.TimeoutSeconds)}
}
