package core

import "fmt"

// EntryExposureEdgeTunnel grants only the Environment's edge-tunnel Component,
// never an application Service or the all-services sentinel.
const EntryExposureEdgeTunnel = "component:edge-tunnel"

func validTunnelTokenKey(value string) bool {
	if value == "" {
		return false
	}
	for index, character := range []byte(value) {
		if character == '_' || character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' ||
			index > 0 && character >= '0' && character <= '9' {
			continue
		}
		return false
	}
	return true
}

// ServiceExposure excludes closed Component grants from workload injection.
func (entry EnvEntry) ServiceExposure() []string {
	result := make([]string, 0, len(entry.Exposure))
	for _, target := range entry.Exposure {
		if target != EntryExposureEdgeTunnel {
			result = append(result, target)
		}
	}
	return result
}

// TunnelTokenEntry selects only a protected, Environment-owned literal Entry
// explicitly granted to this Component. Project references and workload-wide
// exposure cannot be used to satisfy an Environment token selection.
func (environment Environment) TunnelTokenEntry(key string) (EnvEntry, error) {
	for _, entry := range environment.Entries {
		if entry.Key != key {
			continue
		}
		if entry.Kind != EntryKindEnv || !entry.Secret || entry.Source.Kind != SourceLiteral ||
			len(entry.Exposure) != 1 || entry.Exposure[0] != EntryExposureEdgeTunnel {
			return EnvEntry{}, fmt.Errorf(
				"tunnel token must be a protected Environment variable exposed only to the edge-tunnel Component",
			)
		}
		return entry, nil
	}
	return EnvEntry{}, fmt.Errorf("tunnel token Environment variable %q is not declared", key)
}

// ValidateComponentEntryReferences prevents removing or widening a token Entry
// while its Component configuration still selects it. Values remain out of authored input.
func (input BlueprintDesiredInput) ValidateComponentEntryReferences() error {
	for _, spec := range input.Components {
		if spec.Settings.TokenEntry == "" {
			continue
		}
		key := spec.Settings.TokenEntry
		value, exists := input.Entries[key]
		if !exists {
			return fmt.Errorf(
				"tunnel token Environment variable %q is not declared; change the Component credential reference before removing it",
				key,
			)
		}
		entry, err := ProjectEntrySpec(key, value, "ev_00000000000000000000000000")
		if err != nil {
			return err
		}
		if _, err := (Environment{Entries: []EnvEntry{entry}}).TunnelTokenEntry(key); err != nil {
			return err
		}
	}
	return nil
}
