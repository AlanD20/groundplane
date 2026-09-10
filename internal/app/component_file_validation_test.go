package app

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"runtime"
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	registeredcaddy "github.com/AlanD20/groundplane-registered-components/caddy"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelpercontainer"
)

func TestRegisteredComponentFilePreflightUsesPinnedNativeRecipe(t *testing.T) {
	// Rationale: production wiring must connect Environment materialization to
	// the compiled Caddy stdin validator, not only to its later reload action.
	catalog, err := newRegisteredActionCatalog()
	if err != nil {
		t.Fatal(err)
	}
	executor := &componentFileValidationExecutor{}
	validator, err := newComponentFileValidator(catalog, executor)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("http:// { respond 200 }\n")
	envelope := componentFileValidationEnvelope(t, catalog, sha256.Sum256(content))
	if err := validator.ValidateComponentFile(t.Context(), envelope, registeredcaddy.CaddyfileSource, content); err != nil {
		t.Fatal(err)
	}
	platform, reference, found := registeredcaddy.Image.Select(runtime.GOOS, runtime.GOARCH)
	if !found || executor.calls != 1 || executor.image.Reference != reference || executor.image.Platform != platform ||
		!reflect.DeepEqual(
			executor.arguments,
			[]string{"caddy", "validate", "--config", "-", "--adapter", "caddyfile"},
		) ||
		string(executor.content) != string(content) {
		t.Fatalf("native preflight recipe = %+v", executor)
	}
	executor.err = errors.New("native file rejected")
	if err := validator.ValidateComponentFile(t.Context(), envelope, registeredcaddy.CaddyfileSource, content); !errors.Is(
		err,
		executor.err,
	) {
		t.Fatalf("validator error lost: %v", err)
	}
	if err := validator.ValidateComponentFile(t.Context(), envelope, "another/file", content); err == nil ||
		executor.calls != 2 {
		t.Fatalf("wrong destination reached native validator: calls=%d error=%v", executor.calls, err)
	}
}

func componentFileValidationEnvelope(
	t *testing.T,
	catalog registeredActionCatalog,
	digest [32]byte,
) componentsdk.ActionEnvelope {
	t.Helper()
	definition, err := registeredcaddy.Definition()
	if err != nil {
		t.Fatal(err)
	}
	componentID, err := componentsdk.NewComponentID("cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != nil {
		t.Fatal(err)
	}
	artifactID, err := componentsdk.NewArtifactID("cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := componentsdk.NewArtifactReference(artifactID, digest)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := componentsdk.NewActionEnvelope(componentsdk.ActionEnvelopeInput{
		ComponentID: componentID, Artifact: artifact, Generation: 1, DefinitionDigest: definition.Digest(),
		CatalogDigest: catalog.Digest(), ActionID: registeredcaddy.ActivateConfigAction,
	})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

type componentFileValidationExecutor struct {
	calls     int
	image     managedconfighelpercontainer.ValidatorImage
	arguments []string
	content   []byte
	err       error
}

func (executor *componentFileValidationExecutor) ValidateToExit(
	_ context.Context, image managedconfighelpercontainer.ValidatorImage, arguments []string, content []byte,
) error {
	executor.calls++
	executor.image, executor.arguments, executor.content = image, arguments, append([]byte(nil), content...)
	return executor.err
}
