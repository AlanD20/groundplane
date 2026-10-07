package composerender

import (
	"sort"
	"strconv"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

func applyServiceArtifactSettings(node *yaml.Node, settings *ServiceArtifactSettingsMutation) error {
	if settings == nil {
		return nil
	}
	if node == nil || node.Kind != yaml.MappingNode {
		return errs.New(errs.KindInternal, "normalized Compose Service is corrupt")
	}
	if settings.Command != nil {
		setServiceStringSequence(node, "command", *settings.Command)
	}
	if settings.Entrypoint != nil {
		setServiceStringSequence(node, "entrypoint", *settings.Entrypoint)
	}
	if settings.WorkingDir != nil {
		setOrRemoveMappingScalar(node, "working_dir", *settings.WorkingDir)
	}
	if settings.User != nil {
		setOrRemoveMappingScalar(node, "user", *settings.User)
	}
	if settings.Aliases != nil {
		if err := setServiceAliases(node, *settings.Aliases); err != nil {
			return err
		}
	}
	if settings.DependsOn != nil {
		setServiceDependencies(node, *settings.DependsOn)
	}
	if settings.Logging != nil {
		if err := setServiceLogging(node, *settings.Logging); err != nil {
			return err
		}
	}
	sortMapping(node)
	return nil
}

func setServiceAliases(service *yaml.Node, aliases map[string][]string) error {
	index := MappingIndex(service, "networks")
	if index < 0 || service.Content[index+1].Kind != yaml.MappingNode {
		return errs.New(errs.KindInternal, "normalized Compose Service networks are corrupt")
	}
	networks := service.Content[index+1]
	for offset := 0; offset+1 < len(networks.Content); offset += 2 {
		zone := networks.Content[offset].Value
		attachment := networks.Content[offset+1]
		if attachment.Kind != yaml.MappingNode {
			attachment = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
			networks.Content[offset+1] = attachment
		}
		setServiceStringSequence(attachment, "aliases", aliases[zone])
		sortMapping(attachment)
	}
	return nil
}

func setServiceDependencies(service *yaml.Node, dependencies map[string]core.ServiceDependency) {
	names := make([]string, 0, len(dependencies))
	for name, dependency := range dependencies {
		if len(dependency.Phases) == 0 {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		RemoveMappingValue(service, "depends_on")
		return
	}
	sort.Strings(names)
	value := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	for _, name := range names {
		dependency := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		AppendMappingValue(dependency, "condition", scalarNode(dependencies[name].Condition.String()))
		AppendMappingValue(dependency, "required", &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "true"})
		AppendMappingValue(value, name, dependency)
	}
	setMappingNode(service, "depends_on", value)
}

func setServiceLogging(service *yaml.Node, logging core.ServiceLogging) error {
	index := MappingIndex(service, "logging")
	if index < 0 && logging == (core.ServiceLogging{}) {
		return nil
	}
	configuration := EnsureMappingValue(service, "logging")
	if configuration == nil {
		return errs.New(errs.KindInternal, "normalized Compose Service logging is corrupt")
	}
	options := EnsureMappingValue(configuration, "options")
	if options == nil {
		return errs.New(errs.KindInternal, "normalized Compose Service logging options are corrupt")
	}
	setOrRemoveMappingScalar(options, "max-size", logging.MaxSize)
	if logging.MaxFile == 0 {
		RemoveMappingValue(options, "max-file")
	} else {
		setMappingScalar(options, "max-file", strconv.Itoa(logging.MaxFile))
	}
	if len(options.Content) == 0 {
		RemoveMappingValue(configuration, "options")
	} else {
		sortMapping(options)
	}
	if len(configuration.Content) == 0 {
		RemoveMappingValue(service, "logging")
	} else {
		sortMapping(configuration)
	}
	return nil
}
