// Package environmentfile owns generated environment-file paths and pure
// Compose dotenv rendering from resolved Entry values.
package environmentfile

import (
	"github.com/AlanD20/groundplane/internal/common/dotenvfile"
	"sort"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EnvFileName is the canonical, id-based all-services env file name for
// an environment — renaming the environment's label never changes this
// (blueprint.md, "Generated environment files"): `secrets/.env.<environment-id>`.
func EnvFileName(environmentID string) string {
	return "secrets/.env." + environmentID
}

// ServiceEnvFileName is the id-based, service-specific generated file
// for entries whose Exposure names one or more specific services rather
// than the "all" sentinel.
func ServiceEnvFileName(environmentID, serviceName string) string {
	return "secrets/.env." + environmentID + "." + serviceName
}

// RenderEnvFile serializes an environment's all-services env entries
// into the canonical env file content. It is deliberately PURE
// (architecture.md, "The renderer is pure with respect to its inputs"):
// resolved carries each entry's already-decrypted/already-resolved
// value (entry id -> value), produced by an earlier, infra-touching
// step (the secret store for secret_ref, the facts resolver for a live
// {attach, key} reference — see core.FactRef). This function only
// decides file membership and formatting, never touches secrets storage
// itself.
func RenderEnvFile(entries []core.EnvEntry, resolved map[string]string) ([]byte, error) {
	return renderEnvFile(entries, resolved, func(entry core.EnvEntry) bool {
		return entry.Kind == core.EntryKindEnv && entry.ExposesAll()
	})
}

// RenderServiceEnvFile is RenderEnvFile for one service's generated
// file — entries whose Exposure lists that service name specifically
// (never the "all" sentinel, which belongs in the canonical file).
func RenderServiceEnvFile(serviceName string, entries []core.EnvEntry, resolved map[string]string) ([]byte, error) {
	return renderEnvFile(entries, resolved, func(entry core.EnvEntry) bool {
		return entry.Kind == core.EntryKindEnv && !entry.ExposesAll() && containsString(entry.Exposure, serviceName)
	})
}

func renderEnvFile(
	entries []core.EnvEntry,
	resolved map[string]string,
	include func(core.EnvEntry) bool,
) ([]byte, error) {
	selected := make([]core.EnvEntry, 0, len(entries))
	for _, entry := range entries {
		if include(entry) {
			selected = append(selected, entry)
		}
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].Key == selected[j].Key {
			return selected[i].ID < selected[j].ID
		}
		return selected[i].Key < selected[j].Key
	})

	values := make([]dotenvfile.Value, 0, len(selected))
	defer func() {
		for _, value := range values {
			clear(value.Content)
		}
	}()
	for _, entry := range selected {
		value, found := resolved[entry.ID]
		if !found {
			return nil, errs.Newf(
				errs.KindInternal,
				"renderer: no resolved value for entry %s (%s)",
				entry.ID,
				entry.Key,
			)
		}
		values = append(values, dotenvfile.Value{Name: entry.Key, Content: []byte(value)})
	}
	return dotenvfile.Render(values)
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
