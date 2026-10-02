package backupconfigmaterialization

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/dotenvfile"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
)

// ReadValue returns one owned authenticated selected value by canonical ordinal.
// Controller generation and Agent archive readers are distinct source owners.
type ReadValue func(context.Context, int) (backupconfig.Entry, []byte, error)

func (plan *Plan) Render(ctx context.Context, index int, read ReadValue) ([]byte, error) {
	if ctx == nil || plan == nil || read == nil || index < 0 || index >= len(plan.outputs) {
		return nil, invalid()
	}
	item := plan.outputs[index]
	if item.metadata.OutputKind.Removes() {
		return []byte{}, nil
	}
	var values []dotenvfile.Value
	defer func() {
		for _, value := range values {
			clear(value.Content)
		}
	}()
	var selectedSize uint64
	for _, ordinal := range item.values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		expected := plan.entries[ordinal]
		entry, content, err := read(ctx, ordinal)
		if err != nil {
			clear(content)
			return nil, err
		}
		if entry.ID != expected.ID || entry.Value != expected.Value ||
			uint64(len(content)) != expected.Value.SizeBytes ||
			sha256.Sum256(content) != expected.Value.SHA256 {
			clear(content)
			return nil, invalid()
		}
		if expected.Metadata.Kind == backupconfig.MetadataFile {
			return content, nil
		}
		selectedSize += uint64(len(content)) + uint64(len(expected.Metadata.Environment.Key)) + 4
		if selectedSize > entrymaterialization.MaximumContentBytes {
			clear(content)
			return nil, invalid()
		}
		values = append(values, dotenvfile.Value{Name: expected.Metadata.Environment.Key, Content: content})
	}
	return dotenvfile.Render(values)
}

func (plan *Plan) Header(taskID, stepID string, index int, content []byte) (entrymaterialization.Header, error) {
	if plan == nil || index < 0 || index >= len(plan.outputs) ||
		uint64(len(content)) > entrymaterialization.MaximumContentBytes {
		return entrymaterialization.Header{}, invalid()
	}
	metadata := plan.outputs[index].metadata
	return entrymaterialization.NewHeader(entrymaterialization.HeaderSpec{TaskID: taskID, StepID: stepID,
		EnvironmentID: metadata.EnvironmentID, Generation: metadata.Generation, Destination: metadata.Destination,
		ServiceID: metadata.ServiceID, ServiceName: metadata.ServiceName, OutputKind: metadata.OutputKind,
		UID: metadata.UID, GID: metadata.GID, Mode: metadata.Mode, Length: uint64(len(content)), Digest: sha256.Sum256(content)})
}
