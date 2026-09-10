package cli

import (
	"io"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/scriptpolicy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

const maximumScriptExecutionFileBytes = 64 << 10

type scriptExecutionFile struct {
	Mode, Image, User string
	Volumes           []scriptExecutionVolumeFile
	Entries           []string
}

type scriptExecutionVolumeFile struct {
	Volume, Target string
	ReadOnly       bool
}

type scriptExecutionFileNodes struct {
	Mode    yaml.Node            `yaml:"mode"`
	Image   yaml.Node            `yaml:"image"`
	User    yaml.Node            `yaml:"user"`
	Volumes yaml.Node            `yaml:"volumes"`
	Entries yaml.Node            `yaml:"entries"`
	Unknown map[string]yaml.Node `yaml:",inline"`
}

func decodeScriptExecutionFile(value string) (scriptExecutionFile, error) {
	if len(value) > maximumScriptExecutionFileBytes {
		return scriptExecutionFile{}, invalidScriptExecutionFile()
	}
	var document yaml.Node
	decoder := yaml.NewDecoder(strings.NewReader(value))
	if decoder.Decode(&document) != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode || decoder.Decode(&yaml.Node{}) != io.EOF {
		return scriptExecutionFile{}, invalidScriptExecutionFile()
	}
	var fields scriptExecutionFileNodes
	if document.Content[0].Decode(&fields) != nil || len(fields.Unknown) != 0 {
		return scriptExecutionFile{}, invalidScriptExecutionFile()
	}
	mode, valid := scriptFileString(&fields.Mode)
	if !valid {
		return scriptExecutionFile{}, invalidScriptExecutionFile()
	}
	result := scriptExecutionFile{Mode: mode}
	switch mode {
	case "inherited":
		if fields.Image.Kind != 0 || fields.User.Kind != 0 || fields.Volumes.Kind != 0 || fields.Entries.Kind != 0 {
			return scriptExecutionFile{}, invalidScriptExecutionFile()
		}
		return result, nil
	case "explicit":
	default:
		return scriptExecutionFile{}, invalidScriptExecutionFile()
	}
	var imageValid, userValid bool
	result.Image, imageValid = scriptFileString(&fields.Image)
	result.User, userValid = scriptFileString(&fields.User)
	if !imageValid || !userValid {
		return scriptExecutionFile{}, invalidScriptExecutionFile()
	}
	if fields.Volumes.Kind != 0 {
		volumes := scriptFileNode(&fields.Volumes)
		if volumes.Kind != yaml.SequenceNode || len(volumes.Content) > scriptpolicy.MaximumVolumes {
			return scriptExecutionFile{}, invalidScriptExecutionFile()
		}
		for _, node := range volumes.Content {
			grant, err := scriptFileVolume(node)
			if err != nil {
				return scriptExecutionFile{}, err
			}
			result.Volumes = append(result.Volumes, grant)
		}
	}
	if fields.Entries.Kind != 0 {
		entries := scriptFileNode(&fields.Entries)
		if entries.Kind != yaml.SequenceNode || len(entries.Content) > scriptpolicy.MaximumEntries {
			return scriptExecutionFile{}, invalidScriptExecutionFile()
		}
		for _, node := range entries.Content {
			entry, valid := scriptFileString(node)
			if !valid {
				return scriptExecutionFile{}, invalidScriptExecutionFile()
			}
			result.Entries = append(result.Entries, entry)
		}
	}
	return result, nil
}

func scriptFileVolume(node *yaml.Node) (scriptExecutionVolumeFile, error) {
	var fields struct {
		Volume   yaml.Node            `yaml:"volume"`
		Target   yaml.Node            `yaml:"target"`
		ReadOnly yaml.Node            `yaml:"read_only"`
		Unknown  map[string]yaml.Node `yaml:",inline"`
	}
	node = scriptFileNode(node)
	if node.Kind != yaml.MappingNode || node.Decode(&fields) != nil || len(fields.Unknown) != 0 {
		return scriptExecutionVolumeFile{}, invalidScriptExecutionFile()
	}
	volume, volumeValid := scriptFileString(&fields.Volume)
	target, targetValid := scriptFileString(&fields.Target)
	access := scriptFileNode(&fields.ReadOnly)
	var readOnly bool
	if !volumeValid || !targetValid || access.Kind != yaml.ScalarNode || access.Tag != "!!bool" ||
		access.Decode(&readOnly) != nil {
		return scriptExecutionVolumeFile{}, invalidScriptExecutionFile()
	}
	return scriptExecutionVolumeFile{Volume: volume, Target: target, ReadOnly: readOnly}, nil
}

func scriptFileString(node *yaml.Node) (string, bool) {
	node = scriptFileNode(node)
	return node.Value, node.Kind == yaml.ScalarNode && node.Tag == "!!str" && node.Value != ""
}

func scriptFileNode(node *yaml.Node) *yaml.Node {
	for depth := 0; node != nil && depth <= 16; depth++ {
		if node.Kind != yaml.AliasNode {
			return node
		}
		node = node.Alias
	}
	return &yaml.Node{}
}

func invalidScriptExecutionFile() error {
	return errs.New(
		errs.KindValidationFailed,
		"Script execution file requires one bounded, closed context with typed decisions",
	)
}
