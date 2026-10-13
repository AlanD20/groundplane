package composehelper

import (
	"bytes"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

// Compose CLI performs interpolation even on a Controller-normalized project.
// Quote literal dollars only at this transport boundary, leaving the sealed
// artifact and its runtime witnesses unchanged. Keys are not interpolated by
// Compose; escaping them would change labels or environment-variable names.
func composeCLIInput(canonical []byte) ([]byte, error) {
	if !bytes.ContainsRune(canonical, '$') {
		return bytes.Clone(canonical), nil
	}
	var document yaml.Node
	if err := yaml.Unmarshal(canonical, &document); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if err := quoteComposeValues(&document); err != nil {
		return nil, err
	}
	encoded, err := yaml.Marshal(&document)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(encoded) > 2*executionplan.MaximumArtifactYAMLBytes {
		return nil, errs.New(errs.KindValidationFailed, "Compose CLI input exceeds its transport limit")
	}
	return encoded, nil
}

func quoteComposeValues(node *yaml.Node) error {
	switch node.Kind {
	case yaml.MappingNode:
		for index := 1; index < len(node.Content); index += 2 {
			if err := quoteComposeValues(node.Content[index]); err != nil {
				return err
			}
		}
	case yaml.DocumentNode, yaml.SequenceNode:
		for _, child := range node.Content {
			if err := quoteComposeValues(child); err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		if node.Tag == "!!str" {
			node.Value = strings.ReplaceAll(node.Value, "$", "$$")
		}
	case yaml.AliasNode:
		return errs.New(errs.KindValidationFailed, "sealed Compose CLI input cannot contain YAML aliases")
	}
	return nil
}
