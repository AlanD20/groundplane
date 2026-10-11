package edgeconnectioncheck

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
	return request, request.Validate()
}

func ReadPrivateResult(input io.Reader, request Request) (PrivateResult, error) {
	result, err := decode[PrivateResult](input, MaximumOutputBytes)
	if err != nil {
		return PrivateResult{}, err
	}
	return result, result.Validate(request)
}

func MarshalRequest(request Request) ([]byte, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	value, err := json.Marshal(request)
	if err != nil || len(value) > MaximumInputBytes {
		return nil, errs.New(errs.KindValidationFailed, "Tunnel diagnostic input exceeds its bound")
	}
	return value, nil
}

func WritePrivateResult(output io.Writer, result PrivateResult, request Request) error {
	if output == nil || result.Validate(request) != nil {
		return errs.New(errs.KindValidationFailed, "Tunnel diagnostic result is invalid")
	}
	value, err := json.Marshal(result)
	if err != nil || len(value) > MaximumOutputBytes {
		return errs.New(errs.KindInternal, "Tunnel diagnostic output exceeds its bound")
	}
	if _, err := output.Write(append(value, '\n')); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func decode[T Request | PrivateResult](input io.Reader, maximum int64) (T, error) {
	var result T
	if input == nil {
		return result, errs.New(errs.KindValidationFailed, "Tunnel diagnostic input is required")
	}
	value, err := io.ReadAll(io.LimitReader(input, maximum+1))
	if err != nil || len(value) == 0 || int64(len(value)) > maximum {
		return result, errs.New(errs.KindValidationFailed, "Tunnel diagnostic message exceeds its bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, errs.New(errs.KindValidationFailed, "Tunnel diagnostic message is invalid")
	}
	var remainder json.RawMessage
	if decoder.Decode(&remainder) != io.EOF {
		return result, errs.New(errs.KindValidationFailed, "Tunnel diagnostic message has trailing data")
	}
	return result, nil
}
