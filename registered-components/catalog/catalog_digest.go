package catalog

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

func catalogDigest(
	definitions []component.Definition,
	images []registeredImage,
	actions []registeredManagedConfigAction,
	containerActions []registeredContainerConfigAction,
	observations []registeredDNSResolverObservation,
) [sha256.Size]byte {
	encoded := appendLength(nil, len("groundplane-component-catalog-v10"))
	encoded = append(encoded, "groundplane-component-catalog-v10"...)
	encoded = appendLength(encoded, len(definitions))
	for _, definition := range definitions {
		implementation := string(definition.Implementation())
		encoded = appendLength(encoded, len(implementation))
		encoded = append(encoded, implementation...)
		digest := definition.Digest()
		encoded = append(encoded, digest[:]...)
	}
	encoded = appendLength(encoded, len(images))
	for _, registered := range images {
		implementation := string(registered.implementation)
		encoded = appendLength(encoded, len(implementation))
		encoded = append(encoded, implementation...)
		encoded = appendCatalogOCIImage(encoded, registered.image)
	}
	encoded = appendLength(encoded, len(actions))
	for _, action := range actions {
		implementation := string(action.implementation)
		encoded = appendLength(encoded, len(implementation))
		encoded = append(encoded, implementation...)
		actionID := string(action.recipe.actionID)
		encoded = appendLength(encoded, len(actionID))
		encoded = append(encoded, actionID...)
		encoded = appendLength(encoded, len(action.recipe.relativePath))
		encoded = append(encoded, action.recipe.relativePath...)
		encoded = appendLength(encoded, len(action.recipe.image.Repository))
		encoded = append(encoded, action.recipe.image.Repository...)
		encoded = appendLength(encoded, len(action.recipe.validateArgs))
		for _, argument := range action.recipe.validateArgs {
			encoded = appendLength(encoded, len(argument))
			encoded = append(encoded, argument...)
		}
	}
	encoded = appendLength(encoded, len(containerActions))
	for _, action := range containerActions {
		implementation := string(action.implementation)
		encoded = appendLength(encoded, len(implementation))
		encoded = append(encoded, implementation...)
		actionID := string(action.recipe.actionID)
		encoded = appendLength(encoded, len(actionID))
		encoded = append(encoded, actionID...)
		for _, value := range []string{action.recipe.relativePath, action.recipe.containerPath} {
			encoded = appendLength(encoded, len(value))
			encoded = append(encoded, value...)
		}
		encoded = appendLength(encoded, len(action.recipe.image.Repository))
		encoded = append(encoded, action.recipe.image.Repository...)
		for _, command := range [][]string{action.recipe.validateArgs, action.recipe.activateArgs} {
			encoded = appendLength(encoded, len(command))
			for _, argument := range command {
				encoded = appendLength(encoded, len(argument))
				encoded = append(encoded, argument...)
			}
		}
	}
	encoded = appendLength(encoded, len(observations))
	for _, observation := range observations {
		implementation := string(observation.implementation)
		encoded = appendLength(encoded, len(implementation))
		encoded = append(encoded, implementation...)
		actionID := string(observation.recipe.actionID)
		encoded = appendLength(encoded, len(actionID))
		encoded = append(encoded, actionID...)
		for _, value := range []string{
			observation.recipe.serviceName, observation.recipe.artifactTarget,
			observation.recipe.listenEndpoint,
			observation.recipe.metricsURL, observation.recipe.reloadMetric,
		} {
			encoded = appendLength(encoded, len(value))
			encoded = append(encoded, value...)
		}
		encoded = appendLength(encoded, len(observation.recipe.image.Repository))
		encoded = append(encoded, observation.recipe.image.Repository...)
	}
	return sha256.Sum256(encoded)
}

func appendCatalogOCIImage(encoded []byte, image component.OCIImage) []byte {
	for _, value := range []string{image.Repository, image.IndexDigest} {
		encoded = appendLength(encoded, len(value))
		encoded = append(encoded, value...)
	}
	encoded = appendLength(encoded, len(image.Platforms))
	for _, platform := range image.Platforms {
		for _, value := range []string{
			platform.OS, platform.Architecture, platform.Variant, platform.ChildDigest, platform.ConfigDigest,
		} {
			encoded = appendLength(encoded, len(value))
			encoded = append(encoded, value...)
		}
	}
	return encoded
}

func appendLength(target []byte, value int) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(value))
	return append(target, encoded[:]...)
}
