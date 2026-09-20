package blueprintparser

import (
	"github.com/compose-spec/compose-go/v2/format"
	"github.com/compose-spec/compose-go/v2/types"
	"strings"
)

func (p *parsePlan) inspectServiceVolumes(baseDir string, raw any) error {
	if raw == nil {
		return nil
	}
	volumes, ok := anyList(raw)
	if !ok {
		return validationError("blueprint service volumes are invalid")
	}
	for _, rawVolume := range volumes {
		kind, source, readOnly, err := canonicalServiceVolume(rawVolume)
		if err != nil {
			return err
		}
		if kind != types.VolumeTypeBind {
			continue
		}
		if source == "" {
			return validationError("blueprint bind source is required")
		}
		if !readOnly {
			return validationError("blueprint bind source must be read-only")
		}
		if err := p.requireRuntimeReference(baseDir, source); err != nil {
			return err
		}
	}
	return nil
}

func canonicalServiceVolume(raw any) (string, string, bool, error) {
	switch volume := raw.(type) {
	case string:
		parsed, err := format.ParseVolume(volume)
		if err != nil {
			return "", "", false, validationError("blueprint service volume is invalid")
		}
		return parsed.Type, parsed.Source, parsed.ReadOnly, nil
	case map[string]any:
		kind, _ := volume["type"].(string)
		source, _ := volume["source"].(string)
		readOnly, _ := volume["read_only"].(bool)
		return kind, source, readOnly, nil
	default:
		return "", "", false, validationError("blueprint service volume is invalid")
	}
}

func (p *parsePlan) inspectVolumes(baseDir string, raw any) error {
	volumes, ok := stringMap(raw)
	if raw == nil || !ok {
		return nil
	}
	for _, rawVolume := range volumes {
		volume, ok := stringMap(rawVolume)
		if !ok {
			continue
		}
		if _, authoredName := volume["name"]; authoredName {
			return validationError("blueprint volume runtime name is Controller-generated")
		}
		driver, _ := volume["driver"].(string)
		if driver != "" && driver != "local" {
			return validationError("blueprint non-local volume drivers are forbidden")
		}
		opts, _ := stringMap(volume["driver_opts"])
		options, _ := opts["o"].(string)
		if driver == "local" && optionContains(options, "bind") {
			if !optionContains(options, "ro") {
				return validationError("blueprint local bind volume must be read-only")
			}
			device, ok := opts["device"].(string)
			if !ok || device == "" {
				return validationError("blueprint local bind volume device is required")
			}
			if err := p.requireRuntimeReference(baseDir, device); err != nil {
				return err
			}
		}
		if driver == "local" && isNetworkFilesystem(opts, options) {
			return validationError("blueprint network volume resources are forbidden")
		}
	}
	return nil
}

func isNetworkFilesystem(options map[string]any, mountOptions string) bool {
	filesystem, _ := options["type"].(string)
	switch strings.ToLower(filesystem) {
	case "nfs", "nfs4", "cifs", "smb", "smb3":
		return true
	}
	return strings.Contains(mountOptions, "addr=")
}

func optionContains(options, expected string) bool {
	for _, option := range strings.Split(options, ",") {
		if strings.TrimSpace(option) == expected {
			return true
		}
	}
	return false
}
