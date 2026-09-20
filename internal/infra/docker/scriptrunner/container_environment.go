package scriptrunner

import (
	"github.com/AlanD20/groundplane/proto/agentpb"
	"sort"
)

func scriptEnvironment(
	values []*agentpb.ScriptStringPair,
	entries []*agentpb.ScriptEntryArtifact,
) []string {
	environment := make(map[string]string, len(values)+len(entries))
	for _, entry := range entries {
		if entry.Binding.Kind == agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV {
			environment[entry.Binding.EnvironmentKey] = string(entry.Value)
		}
	}
	for _, value := range values {
		environment[value.Key] = value.Value
	}
	keys := make([]string, 0, len(environment))
	for key := range environment {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, len(keys))
	for index, key := range keys {
		result[index] = key + "=" + environment[key]
	}
	return result
}

func environmentList(values []*agentpb.ScriptStringPair) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = value.Key + "=" + value.Value
	}
	return result
}

func pairMap(values []*agentpb.ScriptStringPair) map[string]string {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]string, len(values))
	for _, value := range values {
		result[value.Key] = value.Value
	}
	return result
}
