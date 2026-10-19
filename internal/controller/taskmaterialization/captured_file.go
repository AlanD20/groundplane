package taskmaterialization

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"io"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// OpenCapturedFile resolves an immutable source already selected by the
// authenticated operation owner, without reading current desired configuration.
// The returned stream owns its plaintext and must be closed by the caller.
func (resolver *TaskMaterializationResolver) OpenCapturedFile(
	ctx context.Context,
	taskID, stageID string,
	generation uint64,
	record taskmaterialization.Record,
) (entrymaterialization.Header, io.ReadCloser, error) {
	var empty entrymaterialization.Header
	if ctx == nil || resolver == nil || resolver.blueprints == nil || resolver.values == nil ||
		resolver.secrets == nil ||
		resolver.components == nil ||
		resolver.protector == nil {
		return empty, nil, errs.New(errs.KindInternal, "captured file resolver is not configured")
	}
	if err := ctx.Err(); err != nil {
		return empty, nil, err
	}
	header, err := CapturedFileHeader(taskID, stageID, generation, record)
	if err != nil {
		return empty, nil, err
	}
	var content []byte
	if record.Source.Kind == taskmaterialization.SourceComponentFile {
		if resolver.materializations == nil {
			return empty, nil, errs.New(errs.KindInternal, "captured Component file storage is unavailable")
		}
		content, err = resolver.materializations.Load(ctx, record, generation)
	} else {
		content, err = resolver.resolveSource(ctx, record.EnvironmentID, record.Source)
	}
	if err != nil {
		clear(content)
		return empty, nil, err
	}
	actual := sha256.Sum256(content)
	expected := header.Digest()
	if uint64(len(content)) != record.Length || subtle.ConstantTimeCompare(actual[:], expected[:]) != 1 {
		clear(content)
		return empty, nil, errs.New(
			errs.KindStateConflict,
			"captured file source no longer matches its recorded content",
		)
	}
	return header, entrymaterialization.OwnBytes(content), nil
}

func CapturedFileHeader(
	taskID, stageID string,
	generation uint64,
	record taskmaterialization.Record,
) (entrymaterialization.Header, error) {
	if err := taskmaterialization.ValidateRecord(record, generation); err != nil {
		return entrymaterialization.Header{}, err
	}
	kind, err := entrymaterialization.ParseOutputKind(string(record.OutputKind))
	if err != nil {
		return entrymaterialization.Header{}, err
	}
	expected, err := hex.DecodeString(record.SHA256)
	if err != nil || len(expected) != sha256.Size {
		return entrymaterialization.Header{}, CorruptSource()
	}
	var digest entrymaterialization.Digest
	copy(digest[:], expected)
	return entrymaterialization.NewHeader(entrymaterialization.HeaderSpec{TaskID: taskID, StepID: stageID,
		EnvironmentID: record.EnvironmentID, Generation: generation, Destination: record.Destination, ServiceID: record.ServiceID,
		ServiceName: record.ServiceName, OutputKind: kind, UID: record.UID, GID: record.GID,
		Mode: entrymaterialization.Mode(record.Mode), Length: record.Length, Digest: digest})
}
