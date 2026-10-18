// Package managedconfighelper owns the closed protocol and transactional host write.
package managedconfighelper

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	"google.golang.org/protobuf/proto"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func MarshalRequest(request *agentpb.ManagedConfigHelperRequest) ([]byte, error) {
	owned, err := validateRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(owned.Content)
	return marshalFrame(owned, maximumFramedBytes)
}
func ReadRequest(ctx context.Context, input io.Reader) (*agentpb.ManagedConfigHelperRequest, error) {
	request := &agentpb.ManagedConfigHelperRequest{}
	if err := readFrame(ctx, input, maximumFramedBytes, request); err != nil {
		return nil, err
	}
	return validateRequest(request)
}
func WriteResponse(output io.Writer, response *agentpb.ManagedConfigHelperResponse) error {
	if output == nil || validateResponse(response) != nil {
		return errs.New(errs.KindInternal, "managed-config helper response is invalid")
	}
	framed, err := marshalFrame(response, maximumResponse)
	if err != nil {
		return err
	}
	defer clear(framed)
	return writeAll(context.Background(), output, framed)
}
func ReadResponse(ctx context.Context, input io.Reader) (*agentpb.ManagedConfigHelperResponse, error) {
	response := &agentpb.ManagedConfigHelperResponse{}
	if err := readFrame(ctx, input, maximumResponse, response); err != nil {
		return nil, err
	}
	if err := validateResponse(response); err != nil {
		return nil, err
	}
	return response, nil
}

func marshalFrame(message proto.Message, maximum int) ([]byte, error) {
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	if len(encoded) == 0 || len(encoded) > maximum {
		return nil, errs.New(errs.KindValidationFailed, "managed-config helper frame exceeds its bound")
	}
	framed := make([]byte, frameHeaderBytes+len(encoded))
	binary.BigEndian.PutUint32(framed[:frameHeaderBytes], uint32(len(encoded)))
	copy(framed[frameHeaderBytes:], encoded)
	return framed, nil
}
func readFrame(ctx context.Context, input io.Reader, maximum int, message proto.Message) error {
	if ctx == nil || input == nil {
		return errs.New(errs.KindInternal, "managed-config helper input is not configured")
	}
	header := make([]byte, frameHeaderBytes)
	if _, err := io.ReadFull(input, header); err != nil {
		return errs.New(errs.KindValidationFailed, "managed-config helper frame is incomplete")
	}
	size := binary.BigEndian.Uint32(header)
	if size == 0 || size > uint32(maximum) {
		return errs.New(errs.KindValidationFailed, "managed-config helper frame is invalid")
	}
	encoded := make([]byte, int(size))
	defer clear(encoded)
	if _, err := io.ReadFull(input, encoded); err != nil {
		return errs.New(errs.KindValidationFailed, "managed-config helper frame is incomplete")
	}
	trailing := make([]byte, 1)
	if count, err := input.Read(trailing); count != 0 || err == nil || !errors.Is(err, io.EOF) {
		return errs.New(errs.KindValidationFailed, "managed-config helper frame has trailing bytes")
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		encoded,
		message,
	); err != nil ||
		len(message.ProtoReflect().GetUnknown()) != 0 {
		return errs.New(errs.KindValidationFailed, "managed-config helper protobuf is invalid")
	}
	return ctx.Err()
}
