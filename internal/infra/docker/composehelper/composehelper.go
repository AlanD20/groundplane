// Package composehelper executes native operation-specific Docker Compose
// procedures. Shared framing and CLI primitives do not authorize execution.
package composehelper

import (
	"context"
	"encoding/binary"
	"io"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	SchemaVersion          = 1
	maximumFramedBytes     = executionplan.MaximumPlanBytes + 64*1024
	frameHeaderBytes       = 4
	maximumComponentConfig = 1024 * 1024
)

func frame(encoded []byte) ([]byte, error) {
	if len(encoded) == 0 || len(encoded) > maximumFramedBytes {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper frame length is invalid")
	}
	framed := make([]byte, frameHeaderBytes+len(encoded))
	binary.BigEndian.PutUint32(framed[:frameHeaderBytes], uint32(len(encoded)))
	copy(framed[frameHeaderBytes:], encoded)
	return framed, nil
}

func readFrame(ctx context.Context, input io.Reader) ([]byte, error) {
	if ctx == nil || input == nil {
		return nil, errs.New(errs.KindInternal, "Compose helper frame input is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var header [frameHeaderBytes]byte
	if _, err := io.ReadFull(input, header[:]); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper frame header is incomplete")
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 || length > maximumFramedBytes {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper frame length is invalid")
	}
	encoded := make([]byte, int(length))
	if _, err := io.ReadFull(input, encoded); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper frame payload is incomplete")
	}
	extra, err := io.ReadAll(io.LimitReader(input, 1))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(extra) != 0 {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper frame has trailing bytes")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return encoded, nil
}
