package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"

	environmentcapability "github.com/AlanD20/groundplane/internal/controller/environment"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EnvironmentChanger interface {
	EditEnvironment(
		context.Context,
		string,
		environmentcapability.EditEnvironmentInput,
		string,
	) (etcd.IdempotencyResponse, error)
	RenameEnvironment(
		context.Context,
		string,
		environmentcapability.RenameEnvironmentInput,
		string,
	) (etcd.IdempotencyResponse, error)
}

func (s *Server) editEnvironment(
	ctx context.Context,
	request *environmentEditInput,
) (*environmentMutationOutput, error) {
	if s.environmentChanges == nil {
		return nil, errs.New(errs.KindInternal, "Environment changer is not configured")
	}
	response, err := s.environmentChanges.EditEnvironment(
		ctx,
		request.ID,
		environmentcapability.EditEnvironmentInput{NetworkPool: request.Body.NetworkPool},
		request.IdempotencyKey,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.environmentMutationResponse(response, "edit"), nil
}

func validateEnvironmentEditJSON(body []byte) error {
	_, err := decodeEnvironmentEdit(body)
	return err
}

func decodeEnvironmentEdit(body []byte) (environmentcapability.EditEnvironmentInput, error) {
	if !utf8.Valid(body) {
		return environmentcapability.EditEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment edit body is not valid UTF-8",
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return environmentcapability.EditEnvironmentInput{}, projectCreateJSONError(err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return environmentcapability.EditEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment edit body must be an object",
		)
	}
	input := environmentcapability.EditEnvironmentInput{}
	seen := false
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return environmentcapability.EditEnvironmentInput{}, projectCreateJSONError(err)
		}
		member, ok := token.(string)
		if !ok {
			return environmentcapability.EditEnvironmentInput{}, errs.New(
				errs.KindMalformedRequest,
				"Environment edit member name is invalid",
			)
		}
		if member != "network_pool" {
			return environmentcapability.EditEnvironmentInput{}, errs.New(
				errs.KindMalformedRequest,
				"Environment edit body contains an unknown member",
			)
		}
		if seen {
			return environmentcapability.EditEnvironmentInput{}, errs.New(
				errs.KindMalformedRequest,
				"Environment edit body contains a duplicate member",
			)
		}
		seen = true
		if err := decoder.Decode(&input.NetworkPool); err != nil {
			var typeError *json.UnmarshalTypeError
			if errors.As(err, &typeError) {
				return environmentcapability.EditEnvironmentInput{}, errs.New(
					errs.KindValidationFailed,
					"Environment network_pool must be a string",
				)
			}
			return environmentcapability.EditEnvironmentInput{}, projectCreateJSONError(err)
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return environmentcapability.EditEnvironmentInput{}, projectCreateJSONError(err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return environmentcapability.EditEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment edit body is malformed",
		)
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return environmentcapability.EditEnvironmentInput{}, projectCreateJSONError(err)
	}
	if !seen {
		return environmentcapability.EditEnvironmentInput{}, errs.New(
			errs.KindValidationFailed,
			"Environment network_pool is required",
		)
	}
	if err := environmentcapability.ValidateEnvironmentEditInput(input); err != nil {
		return environmentcapability.EditEnvironmentInput{}, err
	}
	return input, nil
}

func (s *Server) renameEnvironment(
	ctx context.Context,
	request *environmentRenameInput,
) (*environmentMutationOutput, error) {
	if s.environmentChanges == nil {
		return nil, errs.New(errs.KindInternal, "Environment changer is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeEnvironmentRename(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.environmentChanges.RenameEnvironment(
		ctx, request.ID, input, request.IdempotencyKey,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.environmentMutationResponse(response, "rename"), nil
}

func decodeEnvironmentRename(body []byte) (environmentcapability.RenameEnvironmentInput, error) {
	if !utf8.Valid(body) {
		return environmentcapability.RenameEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment rename body is not valid UTF-8",
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return environmentcapability.RenameEnvironmentInput{}, projectCreateJSONError(err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return environmentcapability.RenameEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment rename body must be an object",
		)
	}
	input := environmentcapability.RenameEnvironmentInput{}
	seen := false
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return environmentcapability.RenameEnvironmentInput{}, projectCreateJSONError(err)
		}
		member, ok := token.(string)
		if !ok {
			return environmentcapability.RenameEnvironmentInput{}, errs.New(
				errs.KindMalformedRequest,
				"Environment rename member name is invalid",
			)
		}
		if member != "name" {
			return environmentcapability.RenameEnvironmentInput{}, errs.New(
				errs.KindMalformedRequest,
				"Environment rename body contains an unknown member",
			)
		}
		if seen {
			return environmentcapability.RenameEnvironmentInput{}, errs.New(
				errs.KindMalformedRequest,
				"Environment rename body contains a duplicate member",
			)
		}
		seen = true
		if err := decoder.Decode(&input.Name); err != nil {
			var typeError *json.UnmarshalTypeError
			if errors.As(err, &typeError) {
				return environmentcapability.RenameEnvironmentInput{}, errs.New(
					errs.KindValidationFailed,
					"Environment name must be a string",
				)
			}
			return environmentcapability.RenameEnvironmentInput{}, projectCreateJSONError(err)
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return environmentcapability.RenameEnvironmentInput{}, projectCreateJSONError(err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return environmentcapability.RenameEnvironmentInput{}, errs.New(
			errs.KindMalformedRequest,
			"Environment rename body is malformed",
		)
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return environmentcapability.RenameEnvironmentInput{}, projectCreateJSONError(err)
	}
	if err := environmentcapability.ValidateEnvironmentRenameInput(input); err != nil {
		return environmentcapability.RenameEnvironmentInput{}, err
	}
	return input, nil
}
