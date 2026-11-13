package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type secretEditInput struct {
	ID             string `path:"id" pattern:"^sec_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

func (s *Server) registerSecretEdit(schema *huma.Schema) {
	registerSecretMutation[apiTypes.SecretEditRequest](
		s,
		huma.Operation{
			OperationID:   "secret.edit",
			Method:        http.MethodPatch,
			Path:          "/secrets/{id}",
			Summary:       "Replace a reusable Secret value without changing its stable reference",
			Tags:          []string{"Secret"},
			DefaultStatus: http.StatusOK,
			Middlewares:   huma.Middlewares{s.rejectSecretQuery},
		},
		schema,
		"SecretEditRequest",
		s.editSecret,
	)
	s.setRoutePolicy("PATCH /api/v1/secrets/{id}", routePolicy{body: jsonBody})
}
func decodeSecretEdit(value []byte) (apiTypes.SecretEditRequest, error) {
	var input apiTypes.SecretEditRequest
	invalid := func() (apiTypes.SecretEditRequest, error) {
		return apiTypes.SecretEditRequest{}, errs.New(
			errs.KindMalformedRequest,
			"Secret edit must contain exactly revision and value with valid types",
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	token, err := decoder.Token()
	if err != nil || token != json.Delim(123) {
		return invalid()
	}
	seen := map[string]bool{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return invalid()
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return invalid()
		}
		seen[key] = true
		switch key {
		case "value":
			token, err := decoder.Token()
			text, ok := token.(string)
			if err != nil || !ok {
				return invalid()
			}
			input.Value = text
		case "revision":
			if decoder.Decode(&input.Revision) != nil {
				return invalid()
			}
		default:
			return invalid()
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim(125) || !seen["value"] || !seen["revision"] {
		return invalid()
	}
	if _, err := decoder.Token(); err != io.EOF {
		return invalid()
	}
	return input, nil
}
func (s *Server) editSecret(ctx context.Context, input *secretEditInput) (*secretMutationOutput, error) {
	defer clear(input.RawBody)
	if s.secretMutations == nil {
		return nil, errs.New(errs.KindInternal, "Secret mutations are not configured")
	}
	if len(input.RawBody) > apiTypes.MaximumSecretValueBytes*6+4096 {
		return nil, errs.New(errs.KindRequestTooLarge, "Secret edit body exceeds its input bound")
	}
	body, err := decodeSecretEdit(input.RawBody)
	if err != nil {
		return nil, err
	}
	defer func() { body.Value = "" }()
	response, err := s.secretMutations.EditSecret(ctx, input.ID, body, input.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &secretMutationOutput{
		Status:      response.Status,
		ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetHeader("Content-Length", strconv.Itoa(len(response.Body)))
			_, _ = ctx.BodyWriter().Write(response.Body)
		},
	}, nil
}
