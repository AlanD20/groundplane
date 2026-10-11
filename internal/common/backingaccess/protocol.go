package backingaccess

import (
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	HelperArgument       = "backing-access-check"
	MaximumRequestBytes  = 128 << 10
	MaximumResponseBytes = 4096
)

func MarshalRequest(request Request) ([]byte, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, errs.New(errs.KindInternal, "Backing access request encoding failed")
	}
	defer clear(encoded)
	canonical, err := jcs.Canonicalize(encoded)
	if err != nil || len(canonical) > MaximumRequestBytes {
		clear(canonical)
		return nil, errs.New(errs.KindValidationFailed, "Backing access request exceeds its bound")
	}
	return canonical, nil
}

func ReadRequest(input io.Reader) (Request, error) {
	if input == nil {
		return Request{}, errs.New(errs.KindValidationFailed, "Backing access request input is required")
	}
	encoded, err := io.ReadAll(io.LimitReader(input, MaximumRequestBytes+1))
	defer clear(encoded)
	if err != nil || len(encoded) > MaximumRequestBytes {
		return Request{}, errs.New(
			errs.KindValidationFailed,
			"Backing access request could not be read within its bound",
		)
	}
	request, err := jcs.Decode[Request](encoded)
	if err != nil {
		return Request{}, errs.New(errs.KindValidationFailed, "Backing access request is invalid")
	}
	if err := request.Validate(); err != nil {
		request.Clear()
		return Request{}, err
	}
	return request, nil
}

func WriteResult(output io.Writer, result Result) error {
	if output == nil || result.Validate() != nil {
		return errs.New(errs.KindInternal, "Backing access result is invalid")
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return errs.New(errs.KindInternal, "Backing access result encoding failed")
	}
	canonical, err := jcs.Canonicalize(encoded)
	if err != nil || len(canonical) > MaximumResponseBytes {
		return errs.New(errs.KindInternal, "Backing access result exceeds its bound")
	}
	for len(canonical) != 0 {
		written, err := output.Write(canonical)
		if err != nil || written == 0 {
			return errs.New(errs.KindInternal, "Backing access result could not be written")
		}
		canonical = canonical[written:]
	}
	return nil
}

func ReadResult(input io.Reader) (Result, error) {
	if input == nil {
		return Result{}, errs.New(errs.KindValidationFailed, "Backing access result input is required")
	}
	encoded, err := io.ReadAll(io.LimitReader(input, MaximumResponseBytes+1))
	if err != nil || len(encoded) > MaximumResponseBytes {
		return Result{}, errs.New(errs.KindValidationFailed, "Backing access result could not be read within its bound")
	}
	result, err := jcs.Decode[Result](encoded)
	if err != nil {
		return Result{}, errs.New(errs.KindValidationFailed, "Backing access result is invalid")
	}
	if err := result.Validate(); err != nil {
		return Result{}, err
	}
	return result, nil
}
