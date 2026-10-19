package composehelper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane-component-sdk/catalog"
	component "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	helperPlanID                 = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperArtifactID             = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperServiceID              = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperComponentID            = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperEnvironmentID          = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperTenantID               = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperProjectID              = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	componentConfigRelativePath  = "components/caddy/Caddyfile"
	componentConfigContainerPath = "/etc/caddy/Caddyfile"
	componentImageReference      = "docker.io/library/caddy@sha256:5f5c8640aae01df9654968d946d8f1a56c497f1dd5c5cda4cf95ab7c14d58648"
)

type componentCatalogFake struct{}

func (componentCatalogFake) ResolveContainerConfigAction(
	*agentpb.ComponentApply,
) (ComponentActionRecipe, error) {
	return NewComponentActionRecipe(
		componentConfigRelativePath,
		componentConfigContainerPath,
		componentImageReference,
		component.OCIPlatform{
			OS:           "linux",
			Architecture: "amd64",
			ChildDigest:  strings.Split(componentImageReference, "@sha256:")[1],
			ConfigDigest: strings.Repeat("b", 64),
		},
		[]string{
			"caddy",
			"validate",
			"--config",
			componentConfigContainerPath,
			"--adapter",
			"caddyfile",
		},
		[]string{
			"caddy",
			"reload",
			"--config",
			componentConfigContainerPath,
			"--adapter",
			"caddyfile",
		},
		catalog.ContainerConfigActivationCommand,
	)
}

// Rationale: the helper must prove the exact catalog-generated configuration
// and managed read-only mount before invoking the catalog-owned validation and
// activation recipe in the uniquely running Component container.
func TestExecuteComponentConfigValidatesBeforeActivation(t *testing.T) {
	artifact, action, configPath := componentActivationFixture(t)
	fake := componentConfigRunner(t, configPath, 0)
	recipe, err := (componentCatalogFake{}).ResolveContainerConfigAction(action)
	if err != nil {
		t.Fatal(err)
	}
	response, err := executeComponentContainerConfigAction(t.Context(), fake, 30, artifact, action, recipe)
	if err != nil {
		t.Fatalf("physical Component activation error = %v", err)
	}
	if response.Outcome != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
		response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
		t.Fatalf("response = %#v", response)
	}
	if len(fake.Calls) != 8 {
		t.Fatalf(
			"Runner calls = %d, want lookup, three inspections, two hashes, validate, activate",
			len(fake.Calls),
		)
	}
	if !reflect.DeepEqual(fake.Calls[5].Args, []string{
		"container", "exec", "0123456789abcdef", "caddy", "validate",
		"--config", componentConfigContainerPath, "--adapter", "caddyfile",
	}) || !reflect.DeepEqual(fake.Calls[7].Args, []string{
		"container", "exec", "0123456789abcdef", "caddy", "reload",
		"--config", componentConfigContainerPath, "--adapter", "caddyfile",
	}) {
		t.Fatalf("Component action calls = %#v", fake.Calls[5:])
	}
}

// Rationale: rejected Component configuration must be reported as a bounded
// generic diagnostic and must never reach the activation command.
func TestExecuteComponentConfigRejectionStopsBeforeActivation(t *testing.T) {
	artifact, action, configPath := componentActivationFixture(t)
	fake := componentConfigRunner(t, configPath, 15)
	recipe, err := (componentCatalogFake{}).ResolveContainerConfigAction(action)
	if err != nil {
		t.Fatal(err)
	}
	response, err := executeComponentContainerConfigAction(t.Context(), fake, 30, artifact, action, recipe)
	if err != nil {
		t.Fatalf("physical Component activation error = %v", err)
	}
	if len(fake.Calls) != 6 || response.ExitCode != 15 ||
		response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_CONFIG_REJECTED {
		t.Fatalf("calls=%d response=%#v", len(fake.Calls), response)
	}
}

func TestExecuteComponentConfigRejectsForgedRuntimeAuthorityBeforeActivation(t *testing.T) {
	tests := []struct {
		name              string
		wantStateConflict bool
		mutate            func(*runner.FakeRunner)
	}{
		{name: "incomplete labels", wantStateConflict: true, mutate: func(fake *runner.FakeRunner) {
			base := fake.RunFunc
			fake.RunFunc = func(ctx context.Context, options runner.RunCmdOpts) (runner.Result, error) {
				if strings.Contains(strings.Join(options.Args, " "), ".Config.Labels") {
					return runner.Result{Stdout: []byte("{}\n")}, nil
				}
				return base(ctx, options)
			}
		}},
		{
			name:              "wrong immutable image",
			wantStateConflict: true,
			mutate: func(fake *runner.FakeRunner) {
				base := fake.RunFunc
				fake.RunFunc = func(ctx context.Context, options runner.RunCmdOpts) (runner.Result, error) {
					if strings.Contains(strings.Join(options.Args, " "), ".Config.Image") {
						return runner.Result{
							Stdout: []byte(
								"docker.io/library/caddy@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n",
							),
						}, nil
					}
					return base(ctx, options)
				}
			},
		},
		{name: "container bytes replaced after validation", mutate: func(fake *runner.FakeRunner) {
			base := fake.RunFunc
			hashes := 0
			fake.RunFunc = func(ctx context.Context, options runner.RunCmdOpts) (runner.Result, error) {
				if strings.Contains(strings.Join(options.Args, " "), " sha256sum ") {
					hashes++
					if hashes == 2 {
						return runner.Result{
							Stdout: []byte(
								strings.Repeat(
									"a",
									64,
								) + "  " + componentConfigContainerPath + "\n",
							),
						}, nil
					}
				}
				return base(ctx, options)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			artifact, action, configPath := componentActivationFixture(t)
			fake := componentConfigRunner(t, configPath, 0)
			recipe, err := (componentCatalogFake{}).ResolveContainerConfigAction(action)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(fake)
			response, err := executeComponentContainerConfigAction(t.Context(), fake, 30, artifact, action, recipe)
			if test.wantStateConflict {
				if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || response != nil {
					t.Fatalf("forged runtime result = %#v, %v; want state conflict", response, err)
				}
			} else if err != nil || response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED {
				t.Fatalf("response = %#v", response)
			}
			for _, call := range fake.Calls {
				if strings.Contains(strings.Join(call.Args, " "), " caddy reload ") {
					t.Fatalf("activation executed with forged runtime authority: %#v", call)
				}
			}
		})
	}
}

// CMP-02/03: exercise the active physical activation owner. Native request,
// staged file delivery and Controller settlement are outside this fixture.
func componentActivationFixture(t *testing.T) (*agentpb.ComposeArtifact, *agentpb.ComponentApply, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), helperTenantID, helperProjectID, helperEnvironmentID)
	configPath := filepath.Join(root, filepath.FromSlash(componentConfigRelativePath))
	digest := sha256.Sum256([]byte("http:// {\n\trespond 404\n}\n"))
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: helperArtifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: helperEnvironmentID, ProjectName: "gp-" + strings.ToLower(helperEnvironmentID),
		AuthorizedVolumeDir: root,
		Services: []*agentpb.ComposeService{{
			ServiceId: helperServiceID, ComposeName: "caddy", OwnerComponentId: helperComponentID,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.component-id", Value: helperComponentID},
				{Key: "com.groundplane.environment-id", Value: helperEnvironmentID},
				{Key: "com.groundplane.kind", Value: "service"},
				{Key: "com.groundplane.managed", Value: "true"},
				{Key: "com.groundplane.plan-id", Value: helperPlanID},
				{Key: "com.groundplane.render-generation", Value: "1"},
				{Key: "com.groundplane.service-id", Value: helperServiceID},
			},
		}},
	}
	action := &agentpb.ComponentApply{ComponentId: helperComponentID, ArtifactId: helperArtifactID,
		ArtifactDigest: digest[:], Generation: 1, ActionId: "activate-config"}
	return artifact, action, configPath
}

func componentConfigRunner(t *testing.T, configPath string, validationExit int) *runner.FakeRunner {
	t.Helper()
	labels, err := json.Marshal(map[string]string{
		"com.groundplane.managed": "true", "com.groundplane.kind": "service",
		"com.groundplane.component-id":        helperComponentID,
		"com.groundplane.environment-id":      helperEnvironmentID,
		"com.groundplane.service-id":          helperServiceID,
		"com.groundplane.plan-id":             helperPlanID,
		"com.groundplane.render-generation":   "1",
		"com.groundplane.image-child-digest":  strings.Split(componentImageReference, "@")[1],
		"com.groundplane.image-config-digest": "sha256:" + strings.Repeat("b", 64),
		"com.groundplane.image-index-digest":  "sha256:" + strings.Repeat("0a", 32),
		"com.groundplane.image-platform":      "linux/amd64",
	})
	if err != nil {
		t.Fatalf("Marshal(labels) error = %v", err)
	}
	mounts, err := json.Marshal([]componentConfigMount{
		{
			Type:        "bind",
			Source:      filepath.Dir(configPath),
			Destination: filepath.Dir(componentConfigContainerPath),
			RW:          false,
		},
	})
	if err != nil {
		t.Fatalf("Marshal(mounts) error = %v", err)
	}
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, options runner.RunCmdOpts) (runner.Result, error) {
		joined := strings.Join(options.Args, " ")
		switch {
		case strings.Contains(joined, "container ls"):
			return runner.Result{Stdout: []byte("0123456789abcdef\n")}, nil
		case strings.Contains(joined, ".Config.Labels"):
			return runner.Result{Stdout: append(labels, '\n')}, nil
		case strings.Contains(joined, ".Config.Image"):
			return runner.Result{
				Stdout: []byte(
					componentImageReference + "\nsha256:" + strings.Repeat("b", 64) + "\nnull\n",
				),
			}, nil
		case strings.Contains(joined, ".Mounts"):
			return runner.Result{Stdout: append(mounts, '\n')}, nil
		case strings.Contains(joined, " sha256sum "):
			digest := sha256.Sum256([]byte("http:// {\n\trespond 404\n}\n"))
			return runner.Result{
				Stdout: []byte(
					hex.EncodeToString(digest[:]) + "  " + componentConfigContainerPath + "\n",
				),
			}, nil
		case strings.Contains(joined, " caddy validate ") && validationExit != 0:
			return runner.Result{ExitCode: validationExit}, errors.New("validation failed")
		default:
			return runner.Result{}, nil
		}
	}
	return fake
}
