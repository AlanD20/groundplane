// Package blueprintparser validates the bounded YAML structure of closed
// Blueprint Compose sources before compose-go performs semantic loading.
package blueprintparser

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

const (
	maxYAMLNodes       = 100_000
	maxAliasReferences = 1_000
	maxAliasDepth      = 16
)

// Preflight enforces the accepted aggregate YAML complexity limits over the
// ordered Compose sources of an already structurally validated bundle.
func Preflight(ctx context.Context, bundle core.BlueprintBundle) error {
	if ctx == nil {
		return errs.New(errs.CodeInternal, "blueprint preflight context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(bundle.ComposeSources) == 0 {
		return validationError("blueprint Compose sources are required")
	}

	files := make(map[string][]byte, len(bundle.Files))
	for _, file := range bundle.Files {
		files[file.Path] = file.Content
	}

	totalNodes := 0
	totalAliases := 0
	for _, source := range bundle.ComposeSources {
		if err := ctx.Err(); err != nil {
			return err
		}
		content, ok := files[source]
		if !ok {
			return validationError("blueprint Compose source is missing")
		}

		document, err := decodeSingleDocument(content)
		if err != nil {
			return err
		}
		stats, err := inspectDocument(ctx, document)
		if err != nil {
			return err
		}

		totalNodes += stats.nodes
		if totalNodes > maxYAMLNodes {
			return validationError("blueprint YAML node limit exceeded")
		}
		totalAliases += stats.aliases
		if totalAliases > maxAliasReferences {
			return validationError("blueprint YAML alias reference limit exceeded")
		}
		if stats.aliasDepth > maxAliasDepth {
			return validationError("blueprint YAML alias resolution depth exceeded")
		}
	}
	return nil
}

type documentStats struct {
	nodes      int
	aliases    int
	aliasDepth int
}

func decodeSingleDocument(content []byte) (*yaml.Node, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(content))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, validationError("blueprint Compose source is empty")
		}
		return nil, validationError("blueprint Compose source is invalid YAML")
	}
	if len(document.Content) == 0 {
		return nil, validationError("blueprint Compose source is empty")
	}

	var trailing yaml.Node
	if err := decoder.Decode(&trailing); err == nil {
		return nil, validationError("blueprint Compose source must contain one YAML document")
	} else if !errors.Is(err, io.EOF) {
		return nil, validationError("blueprint Compose source is invalid YAML")
	}
	return &document, nil
}

func inspectDocument(ctx context.Context, document *yaml.Node) (documentStats, error) {
	stats := documentStats{}
	documentNodes := make(map[*yaml.Node]struct{})
	aliases := make([]*yaml.Node, 0)
	stack := []*yaml.Node{document}
	for len(stack) != 0 {
		if err := ctx.Err(); err != nil {
			return documentStats{}, err
		}
		last := len(stack) - 1
		node := stack[last]
		stack = stack[:last]
		if _, seen := documentNodes[node]; seen {
			continue
		}
		documentNodes[node] = struct{}{}
		stats.nodes++
		if node.Kind == yaml.AliasNode {
			stats.aliases++
			aliases = append(aliases, node)
		}
		for index := len(node.Content) - 1; index >= 0; index-- {
			stack = append(stack, node.Content[index])
		}
	}

	for _, alias := range aliases {
		if alias.Alias == nil {
			return documentStats{}, validationError("blueprint YAML alias target is missing")
		}
		if _, ok := documentNodes[alias.Alias]; !ok {
			return documentStats{}, validationError("blueprint YAML alias crosses documents")
		}
	}

	depth, err := aliasGraphDepth(ctx, document)
	if err != nil {
		return documentStats{}, err
	}
	stats.aliasDepth = depth
	return stats, nil
}

type graphFrame struct {
	node           *yaml.Node
	nextEdge       int
	maximum        int
	incomingWeight int
}

func aliasGraphDepth(ctx context.Context, root *yaml.Node) (int, error) {
	const (
		visiting = 1
		visited  = 2
	)
	state := make(map[*yaml.Node]uint8)
	depth := make(map[*yaml.Node]int)
	frames := []graphFrame{{node: root}}
	state[root] = visiting

	for len(frames) != 0 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		frame := &frames[len(frames)-1]
		edgeCount := len(frame.node.Content)
		if frame.node.Kind == yaml.AliasNode {
			edgeCount = 1
		}
		if frame.nextEdge < edgeCount {
			child, weight := graphEdge(frame.node, frame.nextEdge)
			frame.nextEdge++
			switch state[child] {
			case visiting:
				return 0, validationError("blueprint YAML alias cycle detected")
			case visited:
				frame.maximum = max(frame.maximum, weight+depth[child])
			default:
				state[child] = visiting
				frames = append(frames, graphFrame{node: child, incomingWeight: weight})
			}
			continue
		}

		completed := *frame
		depth[completed.node] = completed.maximum
		state[completed.node] = visited
		frames = frames[:len(frames)-1]
		if len(frames) != 0 {
			parent := &frames[len(frames)-1]
			parent.maximum = max(parent.maximum, completed.incomingWeight+completed.maximum)
		}
	}
	return depth[root], nil
}

func graphEdge(node *yaml.Node, index int) (*yaml.Node, int) {
	if node.Kind == yaml.AliasNode {
		return node.Alias, 1
	}
	return node.Content[index], 0
}

func validationError(message string) error {
	return errs.New(errs.CodeValidationFailed, message)
}
