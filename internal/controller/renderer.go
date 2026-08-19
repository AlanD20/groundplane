// renderer.go is the ONE render path: desired state -> Compose /
// Corefile / Caddyfile / env files, validated before reload. Every
// consumer (a tenant deploy, the Router component, CoreDNS's static
// entries) goes through this single, well-tested path — see
// architecture.md, "Unified paths that scale".
package controller

import (
	"bytes"
	"fmt"
	"text/template"

	"github.com/AlanD20/groundplane/internal/core"
)

// RenderCompose maps an Environment's zones/services to a docker-compose
// YAML document, per mvp.md's environment-document mapping table.
// TODO: full field-by-field mapping (healthcheck kinds, mounts, aliases,
// depends_on conditions, logging limits, replicas).
func RenderCompose(env core.Environment) ([]byte, error) {
	return nil, fmt.Errorf("renderer: RenderCompose not implemented")
}

// RenderCorefile renders CoreDNS's config from platform DNS settings
// plus every environment's Caddy split-horizon static entries. Must be
// validated (dry-run parse) before the Agent reloads CoreDNS — an
// invalid Corefile is rejected while the old instance keeps serving
// (zero-downtime, per mvp.md's "DNS resolver (locked)").
func RenderCorefile(zones []DNSZoneEntry, forwarders []DNSForwarder, tailnetDelegation bool) ([]byte, error) {
	const tpl = `{{ range .Zones }}{{ .Host }} IN A {{ .IP }}
{{ end }}
{{ range .Forwarders }}forward {{ .Domain }} {{ range .Resolvers }}{{ . }} {{ end }}
{{ end }}
{{ if .TailnetDelegation }}forward ts.net 100.100.100.100
{{ end }}`
	t, err := template.New("corefile").Parse(tpl)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = t.Execute(&buf, struct {
		Zones             []DNSZoneEntry
		Forwarders        []DNSForwarder
		TailnetDelegation bool
	}{zones, forwarders, tailnetDelegation})
	return buf.Bytes(), err
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
	t, err := template.New("caddyfile").Parse(templateBody)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = t.Execute(&buf, struct{ Slot, Host string }{slot, host})
	return buf.Bytes(), err
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
	var buf bytes.Buffer
	for _, e := range entries {
		if e.Kind != core.EntryKindEnv || !e.ExposesAll() {
			continue
		}
		v, ok := resolved[e.ID]
		if !ok {
			return nil, fmt.Errorf("renderer: no resolved value for entry %s (%s)", e.ID, e.Key)
		}
		fmt.Fprintf(&buf, "%s=%s\n", e.Key, v)
	}
	return buf.Bytes(), nil
}

// RenderServiceEnvFile is RenderEnvFile for one service's generated
// file — entries whose Exposure lists that service name specifically
// (never the "all" sentinel, which belongs in the canonical file).
func RenderServiceEnvFile(serviceName string, entries []core.EnvEntry, resolved map[string]string) ([]byte, error) {
	var buf bytes.Buffer
	for _, e := range entries {
		if e.Kind != core.EntryKindEnv || e.ExposesAll() {
			continue
		}
		if !containsString(e.Exposure, serviceName) {
			continue
		}
		v, ok := resolved[e.ID]
		if !ok {
			return nil, fmt.Errorf("renderer: no resolved value for entry %s (%s)", e.ID, e.Key)
		}
		fmt.Fprintf(&buf, "%s=%s\n", e.Key, v)
	}
	return buf.Bytes(), nil
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
