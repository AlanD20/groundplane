// renderer.go is the ONE render path: desired state -> Compose /
// Corefile / Caddyfile / env files, validated before reload. Every
// consumer (a tenant deploy, the Router component, CoreDNS's static
// entries) goes through this single, well-tested path — see
// architecture.md, "Unified paths that scale".
package controller

import (
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RenderCompose maps an Environment's zones/services to a docker-compose
// YAML document, per mvp.md's environment-document mapping table.
// TODO: full field-by-field mapping (healthcheck kinds, mounts, aliases,
// depends_on conditions, logging limits, replicas).
func RenderCompose(env core.Environment) ([]byte, error) {
	return nil, errs.New(errs.CodeNotImplemented, "Compose renderer is not implemented")
}

// RenderCorefile renders CoreDNS's config from platform DNS settings
// plus every environment's Caddy split-horizon static entries. Must be
// validated (dry-run parse) before the Agent reloads CoreDNS — an
// invalid Corefile is rejected while the old instance keeps serving
// (zero-downtime, per mvp.md's "DNS resolver (locked)").
func RenderCorefile(zones []DNSZoneEntry, forwarders []DNSForwarder, tailnetDelegation bool) ([]byte, error) {
	return nil, errs.New(errs.CodeNotImplemented, "CoreDNS renderer is not implemented")
}

type DNSZoneEntry struct {
	Host string
	IP   string
}

type DNSForwarder struct {
	Domain    string
	Resolvers []string
}

// RenderCaddyfile fills the environment's Caddyfile.template with the
// active {slot}/{host} placeholders. Validated before reload, same as
// Corefile.
func RenderCaddyfile(templateBody string, slot string, host string) ([]byte, error) {
	rendered := strings.NewReplacer(
		"{slot}", slot,
		"{host}", host,
	).Replace(templateBody)
	return []byte(rendered), nil
}

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

func renderEnvFile(entries []core.EnvEntry, resolved map[string]string, include func(core.EnvEntry) bool) ([]byte, error) {
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

	var output []byte
	for i, entry := range selected {
		if i > 0 && selected[i-1].Key == entry.Key {
			return nil, errs.Newf(errs.CodeValidationFailed, "renderer: duplicate env key %q in one exposure scope", entry.Key)
		}
		value, ok := resolved[entry.ID]
		if !ok {
			return nil, errs.Newf(errs.CodeInternal, "renderer: no resolved value for entry %s (%s)", entry.ID, entry.Key)
		}
		output = append(output, entry.Key...)
		output = append(output, '=')
		output = append(output, value...)
		output = append(output, '\n')
	}
	return output, nil
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
