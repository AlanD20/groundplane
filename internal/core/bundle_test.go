package core

import (
	"fmt"
	"strings"
	"testing"
)

func TestBlueprintBundleValidateAcceptsClosedOrderedInput(t *testing.T) {
	// Rationale: the logical parser boundary must preserve source layer order
	// independently from the canonical file-manifest order.
	t.Parallel()

	bundle := validBlueprintBundle()
	bundle.ComposeSources = []string{"blueprint.yaml", "compose/production.yaml", "compose/base.yaml"}
	if err := bundle.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	file, ok := bundle.File("compose/base.yaml")
	if !ok || string(file.Content) != "services: {}\n" {
		t.Fatalf("File() = %+v, %v", file, ok)
	}
}

func TestBlueprintBundleValidateRejectsInvalidPathsAndOrdering(t *testing.T) {
	// Rationale: no bundle member or source may escape, alias, or ambiguously
	// reorder the namespace materialized for compose-go.
	t.Parallel()

	for _, test := range []struct {
		name   string
		mutate func(*BlueprintBundle)
	}{
		{name: "absolute", mutate: func(bundle *BlueprintBundle) { bundle.Files[0].Path = "/blueprint.yaml" }},
		{name: "traversal", mutate: func(bundle *BlueprintBundle) { bundle.Files[0].Path = "../blueprint.yaml" }},
		{name: "non canonical", mutate: func(bundle *BlueprintBundle) { bundle.Files[0].Path = "./blueprint.yaml" }},
		{name: "backslash", mutate: func(bundle *BlueprintBundle) { bundle.Files[0].Path = `dir\blueprint.yaml` }},
		{name: "unsorted files", mutate: func(bundle *BlueprintBundle) { bundle.Files[0], bundle.Files[1] = bundle.Files[1], bundle.Files[0] }},
		{name: "root not first", mutate: func(bundle *BlueprintBundle) {
			bundle.ComposeSources[0], bundle.ComposeSources[1] = bundle.ComposeSources[1], bundle.ComposeSources[0]
		}},
		{name: "undeclared source", mutate: func(bundle *BlueprintBundle) { bundle.ComposeSources[1] = "missing.yaml" }},
		{name: "duplicate source", mutate: func(bundle *BlueprintBundle) { bundle.ComposeSources[1] = bundle.ComposeSources[0] }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bundle := validBlueprintBundle()
			test.mutate(&bundle)
			if err := bundle.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want rejection")
			}
		})
	}
}

func TestBlueprintBundleValidateEnforcesExactSizeLimits(t *testing.T) {
	// Rationale: parser resource bounds are product limits, not deployment
	// defaults, and must reject input before any materialization side effect.
	t.Parallel()

	tooMany := validBlueprintBundle()
	tooMany.Files = make([]BlueprintFile, BlueprintBundleMaxFiles+1)
	for index := range tooMany.Files {
		tooMany.Files[index] = BlueprintFile{Path: fmt.Sprintf("%03d", index)}
	}
	tooMany.RootPath = "000"
	tooMany.ComposeSources = []string{"000"}

	tooLarge := validBlueprintBundle()
	tooLarge.Files[0].Content = make([]byte, BlueprintBundleMaxFileBytes+1)

	totalTooLarge := BlueprintBundle{RootPath: "a", ComposeSources: []string{"a"}}
	for index := range 4 {
		totalTooLarge.Files = append(totalTooLarge.Files, BlueprintFile{
			Path:    string(rune('a' + index)),
			Content: make([]byte, BlueprintBundleMaxFileBytes),
		})
	}

	longPath := validBlueprintBundle()
	longPath.Files[0].Path = strings.Repeat("a", BlueprintBundleMaxPathBytes+1)

	for name, bundle := range map[string]BlueprintBundle{
		"file count": tooMany,
		"file size":  tooLarge,
		"total size": totalTooLarge,
		"path size":  longPath,
	} {
		t.Run(name, func(t *testing.T) {
			if err := bundle.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want limit rejection")
			}
		})
	}
}

func TestBlueprintBundleValidateRejectsInvalidInterpolation(t *testing.T) {
	// Rationale: interpolation is reproducible only when every key follows the
	// Compose variable grammar and every explicit value is safely representable.
	t.Parallel()

	for _, interpolation := range []map[string]string{
		{"1PORT": "8080"},
		{"BAD-KEY": "value"},
		{"VALID": "bad\x00value"},
		{"VALID": string([]byte{0xff})},
	} {
		bundle := validBlueprintBundle()
		bundle.Interpolation = interpolation
		if err := bundle.Validate(); err == nil {
			t.Fatalf("Validate() with interpolation %#v: error = nil", interpolation)
		}
	}
}

func validBlueprintBundle() BlueprintBundle {
	return BlueprintBundle{
		RootPath:       "blueprint.yaml",
		ComposeSources: []string{"blueprint.yaml", "compose/base.yaml"},
		Files: []BlueprintFile{
			{Path: "blueprint.yaml", Content: []byte("kind: environment\nservices: {}\n")},
			{Path: "compose/base.yaml", Content: []byte("services: {}\n")},
			{Path: "compose/production.yaml", Content: []byte("services: {}\n")},
		},
		Interpolation: map[string]string{"PORT": "8080"},
	}
}
