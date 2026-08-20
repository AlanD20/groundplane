package blueprintparser

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the accepted node ceiling must admit exactly 100,000 physical
// YAML nodes and reject the next node without depending on expanded aliases.
func TestPreflightYAMLNodeBoundary(t *testing.T) {
	atLimit := bundleFromSources(flowSequence(99_998))
	if err := Preflight(context.Background(), atLimit); err != nil {
		t.Fatalf("Preflight() at node limit: %v", err)
	}

	overLimit := bundleFromSources(flowSequence(99_999))
	requireValidationError(t, Preflight(context.Background(), overLimit))
}

// Rationale: exactly 1,000 aliases are permitted, while the next reference
// must fail before Compose loading can amplify the graph.
func TestPreflightAliasReferenceBoundary(t *testing.T) {
	atLimit := bundleFromSources(aliasReferences(1_000))
	if err := Preflight(context.Background(), atLimit); err != nil {
		t.Fatalf("Preflight() at alias limit: %v", err)
	}

	overLimit := bundleFromSources(aliasReferences(1_001))
	requireValidationError(t, Preflight(context.Background(), overLimit))
}

// Rationale: the accepted alias-resolution depth is inclusive at 16 and
// rejects a path containing 17 alias edges.
func TestPreflightAliasResolutionDepthBoundary(t *testing.T) {
	atLimit := bundleFromSources(aliasChain(16))
	if err := Preflight(context.Background(), atLimit); err != nil {
		t.Fatalf("Preflight() at alias depth limit: %v", err)
	}

	overLimit := bundleFromSources(aliasChain(17))
	requireValidationError(t, Preflight(context.Background(), overLimit))
}

// Rationale: a recursive anchor would make graph resolution unbounded even
// when its physical node and alias counts are small.
func TestPreflightRejectsAliasCycles(t *testing.T) {
	bundle := bundleFromSources("cycle: &cycle [*cycle]\n")
	requireValidationError(t, Preflight(context.Background(), bundle))
}

// Rationale: limits apply to the whole ordered source set rather than being
// reset for each Compose layer.
func TestPreflightAggregatesAcrossComposeSources(t *testing.T) {
	atLimit := bundleFromSources(aliasReferences(600), aliasReferences(400))
	if err := Preflight(context.Background(), atLimit); err != nil {
		t.Fatalf("Preflight() at aggregate alias limit: %v", err)
	}

	overLimit := bundleFromSources(aliasReferences(600), aliasReferences(401))
	requireValidationError(t, Preflight(context.Background(), overLimit))
}

// Rationale: each declared Compose source is exactly one non-empty YAML
// document; implicit empty or additional documents are ambiguous layers.
func TestPreflightRejectsMissingEmptyAndMultipleDocuments(t *testing.T) {
	tests := []struct {
		name   string
		bundle core.BlueprintBundle
	}{
		{name: "missing source list", bundle: core.BlueprintBundle{}},
		{name: "missing source file", bundle: core.BlueprintBundle{ComposeSources: []string{"missing.yaml"}}},
		{name: "empty source", bundle: bundleFromSources("  # comment only\n")},
		{name: "multiple documents", bundle: bundleFromSources("services: {}\n---\nvolumes: {}\n")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireValidationError(t, Preflight(context.Background(), test.bundle))
		})
	}
}

// Rationale: YAML syntax failures and aliases cannot borrow anchors from a
// different Compose document or source.
func TestPreflightRejectsInvalidYAMLAndCrossDocumentAliases(t *testing.T) {
	tests := []struct {
		name    string
		sources []string
	}{
		{name: "invalid YAML", sources: []string{"services: [\n"}},
		{name: "cross-source alias", sources: []string{"base: &shared value\n", "use: *shared\n"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireValidationError(t, Preflight(context.Background(), bundleFromSources(test.sources...)))
		})
	}
}

// Rationale: cancellation must bound CPU spent traversing attacker-controlled
// input and propagate through the standard context contract.
func TestPreflightHonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Preflight(ctx, bundleFromSources("services: {}\n"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Preflight() error = %v, want context.Canceled", err)
	}
}

// Rationale: parser diagnostics must never echo submitted Blueprint content
// into logs, API errors, or task output.
func TestPreflightDoesNotLeakSourceContent(t *testing.T) {
	const marker = "private-content-marker"
	err := Preflight(context.Background(), bundleFromSources(marker+": [\n"))
	requireValidationError(t, err)
	if strings.Contains(err.Error(), marker) {
		t.Fatalf("Preflight() leaked source content: %v", err)
	}
}

func requireValidationError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Preflight() error = nil, want validation.failed")
	}
	var domainErr *errs.Error
	if !errors.As(err, &domainErr) {
		t.Fatalf("Preflight() error type = %T, want *errs.Error", err)
	}
	if domainErr.Code != errs.CodeValidationFailed {
		t.Fatalf("Preflight() code = %q, want %q", domainErr.Code, errs.CodeValidationFailed)
	}
}

func bundleFromSources(sources ...string) core.BlueprintBundle {
	bundle := core.BlueprintBundle{}
	for index, content := range sources {
		path := fmt.Sprintf("%03d.yaml", index)
		bundle.ComposeSources = append(bundle.ComposeSources, path)
		bundle.Files = append(bundle.Files, core.BlueprintFile{Path: path, Content: []byte(content)})
	}
	if len(bundle.ComposeSources) != 0 {
		bundle.RootPath = bundle.ComposeSources[0]
	}
	return bundle
}

func flowSequence(elements int) string {
	var builder strings.Builder
	builder.Grow(elements*2 + 2)
	builder.WriteByte('[')
	for index := range elements {
		if index != 0 {
			builder.WriteByte(',')
		}
		builder.WriteByte('x')
	}
	builder.WriteString("]\n")
	return builder.String()
}

func aliasReferences(count int) string {
	var builder strings.Builder
	builder.WriteString("anchor: &anchor value\nreferences: [")
	for index := range count {
		if index != 0 {
			builder.WriteByte(',')
		}
		builder.WriteString("*anchor")
	}
	builder.WriteString("]\n")
	return builder.String()
}

func aliasChain(depth int) string {
	var builder strings.Builder
	builder.WriteString("level0: &level0 value\n")
	for level := 1; level <= depth; level++ {
		fmt.Fprintf(&builder, "level%d: &level%d [*level%d]\n", level, level, level-1)
	}
	return builder.String()
}
