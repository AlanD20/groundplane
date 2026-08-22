package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type EntryReader interface {
	GetEntry(context.Context, string) (etcd.Versioned[etcd.EntryRecord], error)
	ListEntries(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.EntryRecord], error)
	RevealEntry(context.Context, string) (string, error)
}

type EntryMutator interface {
	CreateEntry(
		context.Context,
		apiTypes.EntryCreateRequest,
		string,
	) (etcd.IdempotencyResponse, error)
}

type entryCreateInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type entryListInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit       int    `query:"limit"       required:"false"`
	Cursor      string `query:"cursor"      required:"false"`
}

type entryShowInput struct {
	ID string `path:"id" pattern:"^ev_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type entryPageOutput struct {
	Body apiTypes.Page[apiTypes.Entry]
}

type entryOutput struct {
	Body apiTypes.Entry
}

type entryValueOutput struct {
	Body apiTypes.EntryValue
}

type entryMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerEntries() {
	entrySchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.Entry](),
		true,
		"Entry",
	)
	operation := huma.Operation{
		OperationID: "entry.create", Method: http.MethodPost, Path: "/entries",
		Summary: "Create an environment Entry", Tags: []string{"Entry"},
		DefaultStatus: http.StatusCreated, SkipValidateBody: true,
		Middlewares: huma.Middlewares{s.rejectEntryQuery},
	}
	operation.RequestBody = &huma.RequestBody{
		Required: true,
		Content: map[string]*huma.MediaType{
			"application/json": {
				Schema: s.API.OpenAPI().Components.Schemas.Schema(
					reflect.TypeFor[apiTypes.EntryCreateRequest](),
					true,
					"EntryCreateRequest",
				),
			},
		},
	}
	operation.Responses = map[string]*huma.Response{
		strconv.Itoa(http.StatusCreated): {
			Description: http.StatusText(http.StatusCreated),
			Content: map[string]*huma.MediaType{
				"application/json": {Schema: entrySchema},
			},
		},
	}
	huma.Register(s.API, operation, s.createEntry)
	huma.Register(s.API, huma.Operation{
		OperationID: "entry.list", Method: http.MethodGet, Path: "/entries",
		Summary: "List environment entries", Tags: []string{"Entry"},
		Middlewares: huma.Middlewares{s.validateEntryListQuery},
	}, s.listEntries)
	huma.Register(s.API, huma.Operation{
		OperationID: "entry.show", Method: http.MethodGet, Path: "/entries/{id}",
		Summary: "Show environment Entry metadata", Tags: []string{"Entry"},
	}, s.showEntry)
	huma.Register(s.API, huma.Operation{
		OperationID: "entry.reveal", Method: http.MethodGet, Path: "/entries/{id}/value",
		Summary: "Reveal an encrypted Entry value", Tags: []string{"Entry"},
	}, s.revealEntry)
	s.setRoutePolicy("POST /api/v1/entries", routePolicy{body: jsonBody})
}

func (s *Server) createEntry(
	ctx context.Context,
	request *entryCreateInput,
) (*entryMutationOutput, error) {
	if s.entryMutations == nil {
		return nil, errs.New(errs.KindInternal, "Entry mutator is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeEntryCreate(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.entryMutations.CreateEntry(ctx, input, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &entryMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, writeErr := ctx.BodyWriter().Write(response.Body); writeErr != nil && s.Logger != nil {
				s.Logger.Error("controller: write Entry creation response", slog.Any("error", writeErr))
			}
		},
	}, nil
}

func decodeEntryCreate(body []byte) (apiTypes.EntryCreateRequest, error) {
	members, err := decodeEntryJSONObject(body, "Entry creation")
	if err != nil {
		return apiTypes.EntryCreateRequest{}, err
	}
	defer clearEntryJSONMembers(members)
	allowed := map[string]struct{}{
		"environment_id": {}, "type": {}, "key": {}, "path": {}, "uid": {}, "gid": {},
		"source": {}, "exposure": {}, "secret": {},
	}
	for name := range members {
		if _, ok := allowed[name]; !ok {
			return apiTypes.EntryCreateRequest{}, errs.New(
				errs.KindMalformedRequest,
				"Entry creation body contains an unknown member",
			)
		}
	}
	for _, required := range []string{"environment_id", "type", "source", "exposure", "secret"} {
		if _, present := members[required]; !present {
			return apiTypes.EntryCreateRequest{}, errs.Newf(
				errs.KindValidationFailed,
				"Entry creation requires %s",
				required,
			)
		}
	}
	input := apiTypes.EntryCreateRequest{}
	for name, target := range map[string]*string{
		"environment_id": &input.EnvironmentID, "type": &input.Type, "key": &input.Key, "path": &input.Path,
	} {
		if value, present := members[name]; present {
			if err := decodeEntryJSONMember(value, target); err != nil {
				return apiTypes.EntryCreateRequest{}, err
			}
		}
	}
	for name, target := range map[string]**int64{"uid": &input.UID, "gid": &input.GID} {
		if value, present := members[name]; present {
			var decoded int64
			if err := decodeEntryJSONMember(value, &decoded); err != nil {
				return apiTypes.EntryCreateRequest{}, err
			}
			*target = &decoded
		}
	}
	if err := decodeEntryJSONMember(members["exposure"], &input.Exposure); err != nil {
		return apiTypes.EntryCreateRequest{}, err
	}
	if err := decodeEntryJSONMember(members["secret"], &input.Secret); err != nil {
		return apiTypes.EntryCreateRequest{}, err
	}
	source, err := decodeEntrySource(members["source"])
	if err != nil {
		return apiTypes.EntryCreateRequest{}, err
	}
	input.Source = source
	return input, nil
}

func decodeEntrySource(body []byte) (apiTypes.EntrySource, error) {
	members, err := decodeEntryJSONObject(body, "Entry source")
	if err != nil {
		return apiTypes.EntrySource{}, err
	}
	defer clearEntryJSONMembers(members)
	allowed := map[string]struct{}{
		"kind": {}, "literal": {}, "secret_ref": {}, "attach_id": {}, "grant_attach_id": {}, "fact": {},
	}
	for name := range members {
		if _, ok := allowed[name]; !ok {
			return apiTypes.EntrySource{}, errs.New(
				errs.KindMalformedRequest,
				"Entry source contains an unknown member",
			)
		}
	}
	if _, present := members["kind"]; !present {
		return apiTypes.EntrySource{}, errs.New(errs.KindValidationFailed, "Entry source kind is required")
	}
	source := apiTypes.EntrySource{}
	for name, target := range map[string]*string{
		"kind": &source.Kind, "literal": &source.Literal, "secret_ref": &source.SecretRef,
		"attach_id": &source.AttachID, "grant_attach_id": &source.GrantAttachID, "fact": &source.Fact,
	} {
		if value, present := members[name]; present {
			if err := decodeEntryJSONMember(value, target); err != nil {
				return apiTypes.EntrySource{}, err
			}
		}
	}
	return source, nil
}

func decodeEntryJSONObject(body []byte, label string) (map[string]json.RawMessage, error) {
	if !utf8.Valid(body) {
		return nil, errs.New(errs.KindMalformedRequest, label+" body is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return nil, errs.Wrap(errs.KindMalformedRequest, err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return nil, errs.New(errs.KindMalformedRequest, label+" body must be an object")
	}
	members := make(map[string]json.RawMessage)
	for decoder.More() {
		token, tokenErr := decoder.Token()
		if tokenErr != nil {
			clearEntryJSONMembers(members)
			return nil, errs.Wrap(errs.KindMalformedRequest, tokenErr)
		}
		name, ok := token.(string)
		if !ok {
			clearEntryJSONMembers(members)
			return nil, errs.New(errs.KindMalformedRequest, label+" member name is invalid")
		}
		if _, duplicate := members[name]; duplicate {
			clearEntryJSONMembers(members)
			return nil, errs.New(errs.KindMalformedRequest, label+" body contains a duplicate member")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			clearEntryJSONMembers(members)
			return nil, errs.Wrap(errs.KindMalformedRequest, err)
		}
		members[name] = value
	}
	closing, err := decoder.Token()
	if err != nil {
		clearEntryJSONMembers(members)
		return nil, errs.Wrap(errs.KindMalformedRequest, err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		clearEntryJSONMembers(members)
		return nil, errs.New(errs.KindMalformedRequest, label+" body is malformed")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		clearEntryJSONMembers(members)
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return nil, errs.Wrap(errs.KindMalformedRequest, err)
	}
	return members, nil
}

func decodeEntryJSONMember[T any](raw json.RawMessage, target *T) error {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errs.New(errs.KindValidationFailed, "Entry creation member must not be null")
	}
	if err := json.Unmarshal(raw, target); err != nil {
		var typeError *json.UnmarshalTypeError
		if errors.As(err, &typeError) {
			return errs.New(errs.KindValidationFailed, "Entry creation member has an invalid type")
		}
		return errs.Wrap(errs.KindMalformedRequest, err)
	}
	return nil
}

func clearEntryJSONMembers(members map[string]json.RawMessage) {
	for name, value := range members {
		clear(value)
		delete(members, name)
	}
}

func (s *Server) listEntries(ctx context.Context, request *entryListInput) (*entryPageOutput, error) {
	if s.entries == nil {
		return nil, errs.New(errs.KindInternal, "Entry reader is not configured")
	}
	page, err := s.entries.ListEntries(ctx, request.Environment, etcd.PageRequest{
		Limit: request.Limit, Cursor: request.Cursor,
	})
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.Page[apiTypes.Entry]{
		Items: make([]apiTypes.Entry, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = entryResponse(item.Record)
	}
	return &entryPageOutput{Body: response}, nil
}

func (s *Server) showEntry(ctx context.Context, request *entryShowInput) (*entryOutput, error) {
	if s.entries == nil {
		return nil, errs.New(errs.KindInternal, "Entry reader is not configured")
	}
	stored, err := s.entries.GetEntry(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &entryOutput{Body: entryResponse(stored.Record)}, nil
}

func (s *Server) revealEntry(ctx context.Context, request *entryShowInput) (*entryValueOutput, error) {
	if s.entries == nil {
		return nil, errs.New(errs.KindInternal, "Entry reader is not configured")
	}
	value, err := s.entries.RevealEntry(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &entryValueOutput{Body: apiTypes.EntryValue{Value: value}}, nil
}

func entryResponse(record etcd.EntryRecord) apiTypes.Entry {
	entry := record.Entry
	response := apiTypes.Entry{
		ID: entry.ID, Type: string(entry.Kind), Key: entry.Key, Path: entry.Path,
		Source: apiTypes.EntrySource{
			Kind: string(entry.Source.Kind), Literal: entry.Source.Literal, SecretRef: entry.Source.SecretRef,
		},
		Exposure: append([]string(nil), entry.Exposure...), Secret: entry.Secret,
	}
	if entry.UID != nil {
		value := int64(*entry.UID)
		response.UID = &value
	}
	if entry.GID != nil {
		value := int64(*entry.GID)
		response.GID = &value
	}
	if entry.Source.Fact != nil {
		response.Source.AttachID = entry.Source.Fact.Attach
		response.Source.GrantAttachID = entry.Source.Fact.Grant
		response.Source.Fact = entry.Source.Fact.Key
	}
	return response
}

func (s *Server) rejectEntryQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		s.writeEntryProblem(ctx, "Entry request query is invalid")
		return
	}
	next(ctx)
}

func (s *Server) validateEntryListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	query := requestURL.Query()
	for key, values := range query {
		if key != "environment" && key != "limit" && key != "cursor" {
			s.writeEntryProblem(ctx, "Entry list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeEntryProblem(ctx, "Entry list query contains duplicate values")
			return
		}
	}
	environments, present := query["environment"]
	if !present || len(environments) != 1 || environments[0] == "" {
		s.writeEntryProblem(ctx, "Entry list requires one Environment selector")
		return
	}
	next(ctx)
}

func (s *Server) writeEntryProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Entry request problem", slog.Any("error", err))
	}
}
