package runnerconnectioncheck

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func ReadRequest(input io.Reader) (Request, error) {
	request, err := decode[Request](input, MaximumInputBytes)
	if err != nil {
		return Request{}, err
	}
	if err := request.Validate(); err != nil {
		return Request{}, err
	}
	return request, nil
}

func ReadResult(input io.Reader, request Request) (Result, error) {
	result, err := decode[Result](input, MaximumOutputBytes)
	if err != nil {
		return Result{}, err
	}
	if err := result.Validate(request); err != nil {
		return Result{}, err
	}
	return result, nil
}

func WriteResult(output io.Writer, result Result, request Request) error {
	if output == nil || result.Validate(request) != nil {
		return errs.New(errs.KindValidationFailed, "Runner diagnostic result is invalid")
	}
	value, err := json.Marshal(result)
	if err != nil || len(value) > MaximumOutputBytes {
		return errs.New(errs.KindInternal, "Runner diagnostic output exceeds its bound")
	}
	if _, err := output.Write(append(value, '\n')); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func decode[T Request | Result](input io.Reader, maximum int64) (T, error) {
	var target T
	if input == nil {
		return target, errs.New(errs.KindValidationFailed, "Runner diagnostic input is required")
	}
	value, err := io.ReadAll(io.LimitReader(input, maximum+1))
	if err != nil || len(value) == 0 || int64(len(value)) > maximum {
		return target, errs.New(errs.KindValidationFailed, "Runner diagnostic message exceeds its bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&target) != nil {
		return target, errs.New(errs.KindValidationFailed, "Runner diagnostic message is invalid")
	}
	var remainder json.RawMessage
	if decoder.Decode(&remainder) != io.EOF {
		return target, errs.New(errs.KindValidationFailed, "Runner diagnostic message has trailing data")
	}
	return target, nil
}
