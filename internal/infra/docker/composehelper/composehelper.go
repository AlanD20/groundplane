// Package composehelper owns the task-scoped Docker Compose helper protocol
// and its exact CLI procedure. It never owns task policy or observed state.
package composehelper

import (
	"context"
	"encoding/binary"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"io"
	"math"
)

const (
	SchemaVersion          = 1
	maximumFramedBytes     = executionplan.MaximumPlanBytes + 64*1024
	frameHeaderBytes       = 4
	maximumTimeout         = uint32(math.MaxInt32)
	maximumComponentConfig = 1024 * 1024
)

// MarshalRequest returns one complete request frame for helper stdin.
func MarshalRequest(request *agentpb.ComposeHelperRequest) ([]byte, error) {
	owned, _, _, err := validateRequest(request)
	if err != nil {
		return nil, err
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(owned)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return frame(encoded)
}

// ArtifactForRequest returns the one owned artifact authorized for the
// selected helper step. It applies the same validation as frame encoding.
func ArtifactForRequest(request *agentpb.ComposeHelperRequest) (*agentpb.ComposeArtifact, error) {
	_, _, artifact, err := validateRequest(request)
	if err != nil {
		return nil, err
	}
	if artifact == nil {
		return nil, nil
	}
	return proto.Clone(artifact).(*agentpb.ComposeArtifact), nil
}

// ReadRequest decodes exactly one request and rejects trailing stdin bytes.
func ReadRequest(ctx context.Context, input io.Reader) (*agentpb.ComposeHelperRequest, error) {
	encoded, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	request := &agentpb.ComposeHelperRequest{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, request); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper request protobuf is invalid")
	}
	owned, _, _, err := validateRequest(request)
	return owned, err
}

// WriteResponse writes one complete helper response frame.
func WriteResponse(ctx context.Context, output io.Writer, response *agentpb.ComposeHelperResponse) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateResponse(response); err != nil {
		return err
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	framed, err := frame(encoded)
	if err != nil {
		return err
	}
	for len(framed) != 0 {
		written, writeErr := output.Write(framed)
		if writeErr != nil {
			return errs.Wrap(errs.KindInternal, writeErr)
		}
		if written <= 0 {
			return errs.New(errs.KindInternal, "Compose helper response writer made no progress")
		}
		framed = framed[written:]
	}
	return nil
}

// ReadResponse decodes exactly one response from helper stdout.
func ReadResponse(ctx context.Context, input io.Reader) (*agentpb.ComposeHelperResponse, error) {
	encoded, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	response := &agentpb.ComposeHelperResponse{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, response); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper response protobuf is invalid")
	}
	if err := validateResponse(response); err != nil {
		return nil, err
	}
	return response, nil
}

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
