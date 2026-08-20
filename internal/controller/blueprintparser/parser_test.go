package blueprintparser

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: the root alone owns the Groundplane envelope and typed
// extensions, while ordered Compose layers produce one native typed project.
func TestParseReturnsTypedProjectAndRootExtensions(t *testing.T) {
	bundle := parserBundle([]string{"root.yaml", "override.yaml"}, map[string]string{
		"root.yaml": `kind: environment
schema: 1
metadata: {tenant: acme, project: shop, environment: production}
x-gp-routes:
  - {target: web, exposure: internal}
services:
  web: {image: "${IMAGE}"}
`,
		"override.yaml": "services:\n  web: {image: overridden}\n",
	})
	bundle.Interpolation = map[string]string{"IMAGE": "declared"}

	result, err := Parse(context.Background(), bundle)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if result.Envelope.Kind != core.KindDocEnvironment || result.Project.Services["web"].Image != "overridden" {
		t.Fatalf("Parse() result = %+v", result)
	}
	if len(result.Extensions.Routes) != 1 || result.Extensions.Routes[0].Target != "web" {
		t.Fatalf("routes = %+v", result.Extensions.Routes)
	}
	if _, exists := result.Project.Extensions["x-gp-routes"]; exists {
		t.Fatal("root Groundplane extension leaked into native Compose project")
	}
}

// Rationale: interpolation is a closed explicit input and must never inherit
// either the Controller process environment or an ambient .env file.
func TestParseExcludesAmbientEnvironmentAndDotEnv(t *testing.T) {
	ambient := t.TempDir()
	if err := os.WriteFile(filepath.Join(ambient, ".env"), []byte("IMAGE=from-dotenv\n"), 0o600); err != nil {
		t.Fatalf("write ambient .env: %v", err)
	}
	t.Chdir(ambient)
	t.Setenv("IMAGE", "from-process")
	bundle := parserBundle([]string{"root.yaml"}, map[string]string{
		"root.yaml": environmentRoot("services:\n  web: {image: \"${IMAGE:-fallback}\"}\n"),
	})

	result, err := Parse(context.Background(), bundle)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if got := result.Project.Services["web"].Image; got != "fallback" {
		t.Fatalf("image = %q, want fallback", got)
	}
}

// Rationale: even a declared .env that must be materialized for another
// native feature cannot become an implicit interpolation source for include.
func TestParseDisablesImplicitIncludedDotEnv(t *testing.T) {
	bundle := parserBundle([]string{"root.yaml"}, map[string]string{
		".env":       "IMAGE=implicit\n",
		"child.yaml": "services:\n  worker: {image: \"${IMAGE:-fallback}\"}\n",
		"root.yaml": environmentRoot(
			"include: [child.yaml]\nconfigs:\n  dotenv: {file: ./.env}\nservices:\n  web: {image: nginx}\n",
		),
	})

	result, err := Parse(context.Background(), bundle)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if got := result.Project.Services["worker"].Image; got != "fallback" {
		t.Fatalf("included image = %q, want fallback", got)
	}
}

// Rationale: bundle paths are canonical from the bundle root even when the
// first Compose source lives below it; compose-go must read the same file the
// security preflight approved.
func TestParseNestedRootUsesClosedBundlePaths(t *testing.T) {
	bundle := parserBundle([]string{"blueprints/root.yaml"}, map[string]string{
		"blueprints/env/web.env": "FROM_FILE=nested\n",
		"blueprints/root.yaml": environmentRoot(
			"services:\n  web:\n    image: nginx\n    env_file: [./env/web.env]\n",
		),
	})

	result, err := Parse(context.Background(), bundle)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	web := result.Project.Services["web"]
	if value := web.Environment["FROM_FILE"]; value == nil || *value != "nested" {
		t.Fatalf("resolved environment = %+v", web.Environment)
	}
	if got := web.EnvFiles[0].Path; got != "blueprints/env/web.env" {
		t.Fatalf("env_file path = %q, want canonical bundle path", got)
	}
}

// Rationale: include env_file is a declared, non-secret interpolation input
// for that included project only and retains native Compose behavior.
func TestParseIncludeUsesDeclaredEnvironmentFile(t *testing.T) {
	bundle := parserBundle([]string{"root.yaml"}, map[string]string{
		"child/compose.yaml": "services:\n  worker: {image: \"${CHILD_IMAGE}\"}\n",
		"root.yaml": environmentRoot(
			"include:\n  - path: child/compose.yaml\n    env_file: vars/child.env\nservices:\n  web: {image: nginx}\n",
		),
		"vars/child.env": "CHILD_IMAGE=busybox\n",
	})

	result, err := Parse(context.Background(), bundle)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if got := result.Project.Services["worker"].Image; got != "busybox" {
		t.Fatalf("included image = %q, want busybox", got)
	}
}

// Rationale: the Controller-derived environment name is the stable Compose
// project identity; authored names may express it but cannot override it.
func TestParseEnforcesControllerProjectName(t *testing.T) {
	matching := parserBundle([]string{"root.yaml"}, map[string]string{
		"root.yaml": environmentRoot("name: Production\nservices:\n  web: {image: nginx}\n"),
	})
	result, err := Parse(context.Background(), matching)
	if err != nil {
		t.Fatalf("Parse() matching normalized project name: %v", err)
	}
	if result.Project.Name != "production" {
		t.Fatalf("project name = %q, want production", result.Project.Name)
	}

	conflicting := parserBundle([]string{"root.yaml"}, map[string]string{
		"root.yaml": environmentRoot("name: another-environment\nservices:\n  web: {image: nginx}\n"),
	})
	requireValidationError(t, parseError(conflicting))
}

// Rationale: native Compose short bind syntax must pass through the same
// compose-go canonical model and closed-bundle checks as long syntax.
func TestParseAcceptsReadOnlyShortBind(t *testing.T) {
	bundle := parserBundle([]string{"root.yaml"}, map[string]string{
		"data/content.txt": "declared\n",
		"root.yaml":        environmentRoot("services:\n  web:\n    image: nginx\n    volumes: [./data:/data:ro]\n"),
	})
	result, err := Parse(context.Background(), bundle)
	if err != nil {
		t.Fatalf("Parse() read-only short bind: %v", err)
	}
	volume := result.Project.Services["web"].Volumes[0]
	if volume.Type != "bind" || !volume.ReadOnly || volume.Source != "data" {
		t.Fatalf("resolved volume = %+v", volume)
	}
}

// Rationale: a bundle-root directory reference means every declared file and
// never files from the Controller's ambient working directory.
func TestRootDirectoryReferenceMaterializesOnlyDeclaredBundle(t *testing.T) {
	bundle := parserBundle([]string{"root.yaml"}, map[string]string{
		"declared.txt":    "root content\n",
		"nested/item.txt": "nested content\n",
		"root.yaml": environmentRoot(
			"services:\n  web:\n    image: nginx\n    volumes: [.:/bundle:ro]\n",
		),
	})
	root, _ := bundle.File(bundle.RootPath)
	plan := newParsePlan(bundle, root.Content, "production", "empty-env-sentinel")
	if _, _, err := plan.requireReference(".", ".", true, false); err != nil {
		t.Fatalf("requireReference() bundle root: %v", err)
	}

	ambient := t.TempDir()
	if err := os.WriteFile(filepath.Join(ambient, "ambient.txt"), []byte("ambient\n"), 0o600); err != nil {
		t.Fatalf("write ambient file: %v", err)
	}
	t.Chdir(ambient)
	workspace := t.TempDir()
	if err := plan.materialize(workspace); err != nil {
		t.Fatalf("materialize() bundle root: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(workspace, "declared.txt"))
	if err != nil || string(content) != "root content\n" {
		t.Fatalf("declared root content = %q, %v", content, err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "nested", "item.txt")); err != nil {
		t.Fatalf("declared nested content is missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "ambient.txt")); !os.IsNotExist(err) {
		t.Fatalf("ambient file materialized, stat error = %v", err)
	}
}

// Rationale: ordinary engine-local named volumes remain valid while explicit
// third-party drivers cannot introduce an undeclared host or network surface.
func TestParseAllowsOrdinaryLocalNamedVolume(t *testing.T) {
	bundle := parserBundle([]string{"root.yaml"}, map[string]string{
		"root.yaml": environmentRoot(
			"services:\n  web: {image: nginx, volumes: [data:/data]}\nvolumes:\n  data: {}\n",
		),
	})
	if _, err := Parse(context.Background(), bundle); err != nil {
		t.Fatalf("Parse() local named volume: %v", err)
	}
}

// Rationale: declared bundle-relative includes, env files, label files,
// non-secret configs, and read-only content binds form the complete file-read surface.
func TestParseLoadsOnlyDeclaredBundleReferences(t *testing.T) {
	bundle := parserBundle([]string{"root.yaml"}, map[string]string{
		"config/app.txt": "configured\n",
		"env/web.env":    "FROM_FILE=yes\n",
		"labels/web":     "com.example.source=bundle\n",
		"root.yaml": environmentRoot(`include: child.yaml
configs:
  app: {file: ./config/app.txt}
services:
  web:
    image: nginx
    env_file: [./env/web.env]
    label_file: [./labels/web]
    configs: [app]
    volumes:
      - {type: bind, source: ./config/app.txt, target: /app.txt, read_only: true}
`),
		"child.yaml": "services:\n  worker: {image: busybox}\n",
	})

	result, err := Parse(context.Background(), bundle)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	web := result.Project.Services["web"]
	if value := web.Environment["FROM_FILE"]; value == nil || *value != "yes" {
		t.Fatalf("resolved environment = %+v", web.Environment)
	}
	if web.Labels["com.example.source"] != "bundle" {
		t.Fatalf("resolved labels = %+v", web.Labels)
	}
	if _, ok := result.Project.Services["worker"]; !ok {
		t.Fatal("included service was not loaded")
	}
	if filepath.IsAbs(web.Volumes[0].Source) || strings.Contains(web.Volumes[0].Source, "groundplane-blueprint") {
		t.Fatalf("returned bind source = %q, want bundle-relative path", web.Volumes[0].Source)
	}
}

// Rationale: references outside the closed namespace, authored secret
// sources, and writable binds must fail before compose-go can read a path.
func TestParseRejectsUnsafeFileSurfaces(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "undeclared env file", body: "services:\n  web:\n    image: nginx\n    env_file: [missing.env]\n"},
		{name: "traversal", body: "include: ../outside.yaml\nservices: {}\n"},
		{name: "secret file", body: "services: {}\nsecrets:\n  token: {file: secret.txt}\n"},
		{name: "secret content", body: "services: {}\nsecrets:\n  token: {content: private}\n"},
		{name: "secret environment", body: "services: {}\nsecrets:\n  token: {environment: TOKEN}\n"},
		{name: "writable bind", body: "services:\n  web:\n    image: nginx\n    volumes: [./data:/data]\n"},
		{name: "host device", body: "services:\n  web:\n    image: nginx\n    devices: [/dev/null:/dev/null]\n"},
		{name: "build", body: "services:\n  web:\n    build: .\n"},
		{name: "published port", body: "services:\n  web:\n    image: nginx\n    ports: [8080:80]\n"},
		{name: "remote include", body: "include: [https://example.com/compose.yaml]\nservices: {}\n"},
		{name: "network volume", body: "services:\n  web: {image: nginx, volumes: [data:/data]}\nvolumes:\n  data:\n    driver: local\n    driver_opts: {type: nfs, o: addr=10.0.0.2, device: :/data}\n"},
		{name: "non-local volume driver", body: "services:\n  web: {image: nginx, volumes: [data:/data]}\nvolumes:\n  data: {driver: arbitrary-plugin}\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			files := map[string]string{"root.yaml": environmentRoot(test.body), "secret.txt": "private", "data": "x"}
			_, err := Parse(context.Background(), parserBundle([]string{"root.yaml"}, files))
			requireValidationError(t, err)
		})
	}
}

// Rationale: YAML merge keys are resolved by compose-go before inspection so
// they cannot hide an absolute or otherwise ambient file read.
func TestParseRejectsUnsafeReferenceHiddenByYAMLMerge(t *testing.T) {
	bundle := parserBundle([]string{"root.yaml"}, map[string]string{
		"root.yaml": environmentRoot(`x-service: &service
  env_file: [/etc/passwd]
services:
  web:
    <<: *service
    image: nginx
`),
	})
	requireValidationError(t, parseError(bundle))
}

// Rationale: unknown keys inside a typed Groundplane extension are rejected
// instead of being silently discarded by the YAML decoder.
func TestParseRejectsUnknownTypedExtensionField(t *testing.T) {
	bundle := parserBundle([]string{"root.yaml"}, map[string]string{
		"root.yaml": environmentRoot(
			"x-gp-routes:\n  - target: web\n    exposure: internal\n    typo: value\nservices:\n  web: {image: nginx}\n",
		),
	})
	requireValidationError(t, parseError(bundle))
}

// Rationale: include and extends resolution is accepted through depth 16 but
// cannot create a deeper loader graph.
func TestParseIncludeDepthBoundary(t *testing.T) {
	atLimit := includeChainBundle(16)
	if _, err := Parse(context.Background(), atLimit); err != nil {
		t.Fatalf("Parse() at include depth limit: %v", err)
	}
	requireValidationError(t, parseError(includeChainBundle(17)))
}

// Rationale: the accepted aggregate resource ceiling counts Compose's
// resolved implicit default network as well as authored resources.
func TestParseResolvedResourceBoundary(t *testing.T) {
	if _, err := Parse(context.Background(), serviceBundle(511)); err != nil {
		t.Fatalf("Parse() at resource limit: %v", err)
	}
	requireValidationError(t, parseError(serviceBundle(512)))
}

// Rationale: errors returned across the parser boundary must not disclose
// submitted Blueprint content even when compose-go includes it in diagnostics.
func TestParseDoesNotLeakContent(t *testing.T) {
	const marker = "private-compose-marker"
	bundle := parserBundle([]string{"root.yaml"}, map[string]string{
		"root.yaml": environmentRoot("services: " + marker + "\n"),
	})
	_, err := Parse(context.Background(), bundle)
	requireValidationError(t, err)
	if strings.Contains(err.Error(), marker) {
		t.Fatalf("Parse() leaked source content: %v", err)
	}
}

func parseError(bundle core.BlueprintBundle) error {
	_, err := Parse(context.Background(), bundle)
	return err
}

func parserBundle(sources []string, contents map[string]string) core.BlueprintBundle {
	paths := make([]string, 0, len(contents))
	for path := range contents {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	bundle := core.BlueprintBundle{RootPath: sources[0], ComposeSources: append([]string(nil), sources...)}
	for _, path := range paths {
		bundle.Files = append(bundle.Files, core.BlueprintFile{Path: path, Content: []byte(contents[path])})
	}
	return bundle
}

func environmentRoot(body string) string {
	return "kind: environment\nschema: 1\nmetadata: {tenant: acme, project: shop, environment: production}\n" + body
}

func includeChainBundle(depth int) core.BlueprintBundle {
	files := map[string]string{}
	for level := 0; level <= depth; level++ {
		path := fmt.Sprintf("%02d.yaml", level)
		body := fmt.Sprintf("services:\n  service%d: {image: busybox}\n", level)
		if level < depth {
			body = fmt.Sprintf("include: %02d.yaml\n", level+1) + body
		}
		if level == 0 {
			body = environmentRoot(body)
		}
		files[path] = body
	}
	return parserBundle([]string{"00.yaml"}, files)
}

func serviceBundle(count int) core.BlueprintBundle {
	var body strings.Builder
	body.WriteString("services:\n")
	for index := range count {
		fmt.Fprintf(&body, "  service%d: {image: busybox}\n", index)
	}
	return parserBundle([]string{"root.yaml"}, map[string]string{"root.yaml": environmentRoot(body.String())})
}
