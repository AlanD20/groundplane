package composehelper

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

// Pin only the selected consumer to its captured local image. Never resolve a
// tag again, mutate the stored artifact, or change another service's definition.
func backupConsumerCLIInput(input executionplan.BackupConsumerInput) ([]byte, error) {
	if input.Artifact == nil || input.Service == nil || input.Fact == nil ||
		len(input.Fact.LocalImageIdSha256) != sha256.Size {
		return nil, invalidBackupConsumerInput()
	}
	var document yaml.Node
	if err := yaml.Unmarshal(input.Artifact.CanonicalYaml, &document); err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	if document.Kind != yaml.DocumentNode || len(document.Content) != 1 {
		return nil, invalidBackupConsumerInput()
	}
	services, err := backupConsumerMappingValue(document.Content[0], "services")
	if err != nil {
		return nil, err
	}
	service, err := backupConsumerMappingValue(services, input.Service.ComposeName)
	if err != nil {
		return nil, err
	}
	image, err := backupConsumerMappingValue(service, "image")
	if err != nil {
		return nil, err
	}
	if image.Kind != yaml.ScalarNode || image.Tag != "!!str" || image.Value != input.Service.ImageReference {
		return nil, invalidBackupConsumerInput()
	}
	image.Value = "sha256:" + hex.EncodeToString(input.Fact.LocalImageIdSha256)
	if err := quoteComposeValues(&document); err != nil {
		return nil, err
	}
	encoded, err := yaml.Marshal(&document)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(encoded) > 2*executionplan.MaximumArtifactYAMLBytes {
		clear(encoded)
		return nil, invalidBackupConsumerInput()
	}
	return encoded, nil
}

func backupConsumerMappingValue(node *yaml.Node, key string) (*yaml.Node, error) {
	if node == nil || node.Kind != yaml.MappingNode || len(node.Content)%2 != 0 {
		return nil, invalidBackupConsumerInput()
	}
	var found *yaml.Node
	for index := 0; index < len(node.Content); index += 2 {
		if node.Content[index].Kind != yaml.ScalarNode || node.Content[index].Tag != "!!str" {
			return nil, invalidBackupConsumerInput()
		}
		if node.Content[index].Value == key {
			if found != nil {
				return nil, invalidBackupConsumerInput()
			}
			found = node.Content[index+1]
		}
	}
	if found == nil {
		return nil, invalidBackupConsumerInput()
	}
	return found, nil
}

func invalidBackupConsumerInput() error {
	return errs.New(errs.KindStateConflict, "backup consumer Compose input differs from its captured workload")
}
