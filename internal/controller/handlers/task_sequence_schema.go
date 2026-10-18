package handlers

import "github.com/danielgtaylor/huma/v2"

// Task sequence cursors are unsigned on the wire and in the durable journal.
// Preserve that range in generated CLI requests instead of narrowing to int64.
type taskSequence uint64

func (taskSequence) Schema(huma.Registry) *huma.Schema {
	minimum := float64(0)
	return &huma.Schema{
		Type: huma.TypeInteger, Format: "uint64", Minimum: &minimum,
		Extensions: map[string]any{"x-go-type": "uint64"},
	}
}
