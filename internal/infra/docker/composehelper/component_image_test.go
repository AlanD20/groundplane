package composehelper

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: containerd reports the child manifest as the actual image ID.
func TestComponentConfigAcceptsSealedContainerdImage(t *testing.T) {
	artifact, action, configPath := componentActivationFixture(t)
	fake := componentConfigRunner(t, configPath, 0)
	recipe, err := (componentCatalogFake{}).ResolveContainerConfigAction(action)
	if err != nil {
		t.Fatal(err)
	}
	base := fake.RunFunc
	child := strings.Split(componentImageReference, "@")[1]
	descriptor, err := json.Marshal(
		ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageManifest,
			Size:      123,
			Digest:    digest.Digest(child),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	fake.RunFunc = func(ctx context.Context, options runner.RunCmdOpts) (runner.Result, error) {
		if strings.Contains(strings.Join(options.Args, " "), ".Config.Image") {
			return runner.Result{
				Stdout: []byte(
					componentImageReference + "\n" + child + "\n" + string(descriptor) + "\n",
				),
			}, nil
		}
		return base(ctx, options)
	}
	response, err := executeComponentContainerConfigAction(t.Context(), fake, 30, artifact, action, recipe)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED {
		t.Fatalf("sealed containerd image rejected: %v", response)
	}
}

// Rationale: a configured OCI reference is not proof of the actual container
// image. A substituted image must never receive validation or activation exec.
func TestComponentConfigRejectsSubstitutedActualImage(t *testing.T) {
	artifact, action, configPath := componentActivationFixture(t)
	fake := componentConfigRunner(t, configPath, 0)
	recipe, err := (componentCatalogFake{}).ResolveContainerConfigAction(action)
	if err != nil {
		t.Fatal(err)
	}
	base := fake.RunFunc
	fake.RunFunc = func(ctx context.Context, options runner.RunCmdOpts) (runner.Result, error) {
		if strings.Contains(strings.Join(options.Args, " "), "{{.Image}}") {
			return runner.Result{
				Stdout: []byte(
					componentImageReference + "\nsha256:" + strings.Repeat("f", 64) + "\nnull\n",
				),
			}, nil
		}
		return base(ctx, options)
	}
	response, err := executeComponentContainerConfigAction(t.Context(), fake, 30, artifact, action, recipe)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || response != nil {
		t.Fatalf("substituted image result = %v, %v; want state conflict", response, err)
	}
	for _, call := range fake.Calls {
		if strings.Contains(strings.Join(call.Args, " "), "container exec") {
			t.Fatalf("executed command in substituted image: %v", call.Args)
		}
	}
}
