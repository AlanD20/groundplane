package app

import (
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: idempotency must compare the verified logical Blueprint rather than unstable multipart framing or map order.
func TestEnvironmentBlueprintIntentManifestIsCanonical(t *testing.T) {
	bundle := core.BlueprintBundle{
		RootPath: "blueprint.yaml", ComposeSources: []string{"blueprint.yaml"},
		Files:         []core.BlueprintFile{{Path: "blueprint.yaml", Content: []byte("services: {}\n")}},
		Interpolation: map[string]string{"ZED": "2", "ALPHA": "1"},
	}
	manifest, err := environmentBlueprintIntentManifest(bundle)
	if err != nil {
		t.Fatalf("environmentBlueprintIntentManifest() error = %v", err)
	}
	if manifest.FormatVersion != 1 || manifest.Files[0].Part != "file-000001" ||
		!reflect.DeepEqual(manifest.Interpolation, []idempotentintent.Interpolation{
			{Name: "ALPHA", Value: "1"}, {Name: "ZED", Value: "2"},
		}) {
		t.Fatalf("manifest = %#v", manifest)
	}
}
