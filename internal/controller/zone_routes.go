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

	"github.com/AlanD20/groundplane/internal/common/ids"
	corenetwork "github.com/AlanD20/groundplane/internal/core/network"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type ZoneReader interface {
	GetZone(context.Context, string) (corenetwork.Zone, error)
	ListZones(context.Context, string, corenetwork.PageRequest) (corenetwork.Page[corenetwork.Zone], error)
}

type ZoneMutator interface {
	CreateZone(context.Context, corenetwork.CreateZoneRequest, string) (corenetwork.MutationResponse, error)
	RemoveZone(context.Context, string, string) (corenetwork.MutationResponse, error)
}

type zoneListInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit       int    `query:"limit" required:"false"`
	Cursor      string `query:"cursor" required:"false"`
}

type zoneShowInput struct {
	ID string `path:"id" pattern:"^net_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type zoneCreateInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type zoneRemoveInput struct {
	ID             string `path:"id" pattern:"^net_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	ImpactToken    string `query:"impact_token" required:"false" pattern:"^[a-f0-9]{64}$"`
}

type zoneRemovalImpactMutator interface {
	RemoveZoneWithImpact(context.Context, string, string, string) (corenetwork.MutationResponse, error)
}

type zoneOutput struct {
	Body apiTypes.Zone
}

type zonePageOutput struct {
	Body apiTypes.Page[apiTypes.Zone]
}

type zoneMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerZones() {
	zoneSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.Zone](),
		true,
		"Zone",
	)
	createOperation := huma.Operation{
		OperationID: "zone.create", Method: http.MethodPost, Path: "/zones",
		Summary: "Create a network zone", Tags: []string{"Zone"}, DefaultStatus: http.StatusCreated,
		Middlewares: huma.Middlewares{s.rejectZoneQuery}, SkipValidateBody: true,
	}
	createOperation.RequestBody = &huma.RequestBody{
		Required: true,
		Content: map[string]*huma.MediaType{
			"application/json": {
				Schema: s.API.OpenAPI().Components.Schemas.Schema(
					reflect.TypeFor[apiTypes.ZoneCreate](),
					true,
					"ZoneCreate",
				),
			},
		},
	}
	createOperation.Responses = map[string]*huma.Response{
		strconv.Itoa(http.StatusCreated): {
			Description: http.StatusText(http.StatusCreated),
			Content: map[string]*huma.MediaType{
				"application/json": {Schema: zoneSchema},
			},
		},
	}
	huma.Register(s.API, createOperation, s.createZone)
	huma.Register(s.API, huma.Operation{
		OperationID: "zone.list", Method: http.MethodGet, Path: "/zones",
		Summary: "List network zones", Tags: []string{"Zone"},
		Middlewares: huma.Middlewares{s.validateZoneListQuery},
	}, s.listZones)
	huma.Register(s.API, huma.Operation{
		OperationID: "zone.show", Method: http.MethodGet, Path: "/zones/{id}",
		Summary: "Show a network zone", Tags: []string{"Zone"},
	}, s.showZone)
	s.registerZoneRemovalImpact()
	taskAcceptedSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](), true, "TaskAccepted",
	)
	huma.Register(s.API, huma.Operation{
		OperationID: "zone.remove", Method: http.MethodDelete, Path: "/zones/{id}",
		Summary: "Remove a network zone", Tags: []string{"Zone"},
		DefaultStatus: http.StatusAccepted,
		Middlewares:   huma.Middlewares{s.rejectZoneDeleteBody, s.validateZoneDeleteQuery},
		Responses:     attachMutationResponses(taskAcceptedSchema),
	}, s.removeZone)
	s.setRoutePolicy("POST /api/v1/zones", routePolicy{body: jsonBody})
}

func (s *Server) removeZone(
	ctx context.Context,
	request *zoneRemoveInput,
) (*zoneMutationOutput, error) {
	if s.zoneMutations == nil {
		return nil, errs.New(errs.KindInternal, "Zone mutator is not configured")
	}
	mutator, ok := s.zoneMutations.(zoneRemovalImpactMutator)
	if !ok {
		if request.ImpactToken != "" {
			return nil, errs.New(errs.KindInternal, "Zone impact mutator is not configured")
		}
		response, err := s.zoneMutations.RemoveZone(ctx, request.ID, request.IdempotencyKey)
		if err != nil {
			return nil, normalizeProjectError(err)
		}
		return s.zoneMutationResponse(response), nil
	}
	response, err := mutator.RemoveZoneWithImpact(
		ctx, request.ID, request.IdempotencyKey, request.ImpactToken,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.zoneMutationResponse(response), nil
}

func (s *Server) createZone(
	ctx context.Context,
	request *zoneCreateInput,
) (*zoneMutationOutput, error) {
	if s.zoneMutations == nil {
		return nil, errs.New(errs.KindInternal, "Zone mutator is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeZoneCreate(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.zoneMutations.CreateZone(ctx, corenetwork.CreateZoneRequest{
		EnvironmentID: input.EnvironmentID,
		Name:          input.Name,
		Subnet:        input.Subnet,
		Internal:      input.Internal,
	}, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.zoneMutationResponse(response), nil
}

func decodeZoneCreate(body []byte) (apiTypes.ZoneCreate, error) {
	if !utf8.Valid(body) {
		return apiTypes.ZoneCreate{}, errs.New(errs.KindMalformedRequest, "Zone creation body is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return apiTypes.ZoneCreate{}, zoneCreateJSONError(err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return apiTypes.ZoneCreate{}, errs.New(errs.KindMalformedRequest, "Zone creation body must be an object")
	}
	input := apiTypes.ZoneCreate{}
	seen := make(map[string]struct{}, 4)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return apiTypes.ZoneCreate{}, zoneCreateJSONError(err)
		}
		key, ok := token.(string)
		if !ok {
			return apiTypes.ZoneCreate{}, errs.New(errs.KindMalformedRequest, "Zone creation member name is invalid")
		}
		if _, duplicate := seen[key]; duplicate {
			return apiTypes.ZoneCreate{}, errs.New(
				errs.KindMalformedRequest,
				"Zone creation body contains a duplicate member",
			)
		}
		seen[key] = struct{}{}
		switch key {
		case "environment_id", "name", "subnet":
			var value string
			if err := decoder.Decode(&value); err != nil {
				var typeError *json.UnmarshalTypeError
				if errors.As(err, &typeError) {
					return apiTypes.ZoneCreate{}, errs.New(
						errs.KindValidationFailed,
						"Zone creation environment_id, name, and subnet must be strings",
					)
				}
				return apiTypes.ZoneCreate{}, zoneCreateJSONError(err)
			}
			switch key {
			case "environment_id":
				input.EnvironmentID = value
			case "name":
				input.Name = value
			case "subnet":
				input.Subnet = value
			}
		case "internal":
			if err := decoder.Decode(&input.Internal); err != nil {
				var typeError *json.UnmarshalTypeError
				if errors.As(err, &typeError) {
					return apiTypes.ZoneCreate{}, errs.New(
						errs.KindValidationFailed,
						"Zone creation internal member must be a boolean",
					)
				}
				return apiTypes.ZoneCreate{}, zoneCreateJSONError(err)
			}
		default:
			return apiTypes.ZoneCreate{}, errs.New(
				errs.KindMalformedRequest,
				"Zone creation body contains an unknown member",
			)
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return apiTypes.ZoneCreate{}, zoneCreateJSONError(err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return apiTypes.ZoneCreate{}, errs.New(errs.KindMalformedRequest, "Zone creation body is malformed")
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return apiTypes.ZoneCreate{}, zoneCreateJSONError(err)
	}
	for _, required := range []string{"environment_id", "name", "subnet", "internal"} {
		if _, present := seen[required]; !present {
			return apiTypes.ZoneCreate{}, errs.New(
				errs.KindValidationFailed,
				"Zone creation body is missing a required member",
			)
		}
	}
	return input, nil
}

func zoneCreateJSONError(err error) error {
	return errs.Wrap(errs.KindMalformedRequest, err)
}

func (s *Server) zoneMutationResponse(response corenetwork.MutationResponse) *zoneMutationOutput {
	return &zoneMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error("controller: write Zone creation response", slog.Any("error", err))
			}
		},
	}
}

func (s *Server) listZones(
	ctx context.Context,
	request *zoneListInput,
) (*zonePageOutput, error) {
	if s.zones == nil {
		return nil, errs.New(errs.KindInternal, "Zone reader is not configured")
	}
	pageRequest, err := zoneListRequest(request.Environment, request.Limit, request.Cursor)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	page, err := s.zones.ListZones(ctx, request.Environment, pageRequest)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.Page[apiTypes.Zone]{
		Items: make([]apiTypes.Zone, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = zoneResponse(item)
	}
	return &zonePageOutput{Body: response}, nil
}

func (s *Server) showZone(
	ctx context.Context,
	request *zoneShowInput,
) (*zoneOutput, error) {
	if s.zones == nil {
		return nil, errs.New(errs.KindInternal, "Zone reader is not configured")
	}
	zone, err := s.zones.GetZone(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &zoneOutput{Body: zoneResponse(zone)}, nil
}

func zoneListRequest(environmentID string, limit int, cursor string) (corenetwork.PageRequest, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return corenetwork.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Zone list requires a stable Environment id",
		)
	}
	if limit < 0 {
		return corenetwork.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Zone list limit must be a positive integer",
		)
	}
	return corenetwork.PageRequest{Limit: limit, Cursor: cursor}, nil
}

func zoneResponse(record corenetwork.Zone) apiTypes.Zone {
	return apiTypes.Zone{
		ID: record.ID, EnvironmentID: record.EnvironmentID, Name: record.Name,
		Subnet: record.Subnet, Internal: record.Internal,
		OwnerKind: apiTypes.ZoneOwnerKind(record.OwnerKind), OwnerID: record.OwnerID,
	}
}

func (s *Server) validateZoneListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	for key, values := range requestURL.Query() {
		if key != "environment" && key != "limit" && key != "cursor" {
			s.writeZoneProblem(ctx, "Zone list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeZoneProblem(ctx, "Zone list query contains duplicate values")
			return
		}
	}
	next(ctx)
}

func (s *Server) rejectZoneQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		s.writeZoneProblem(ctx, "Zone request query is invalid")
		return
	}
	next(ctx)
}

func (s *Server) validateZoneDeleteQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	for key, values := range requestURL.Query() {
		if key != "impact_token" || len(values) != 1 {
			s.writeZoneProblem(ctx, "Zone deletion query is invalid")
			return
		}
	}
	next(ctx)
}

func (s *Server) rejectZoneDeleteBody(ctx huma.Context, next func(huma.Context)) {
	var probe [1]byte
	count, err := ctx.BodyReader().Read(probe[:])
	if count != 0 || (err != nil && !errors.Is(err, io.EOF)) {
		s.writeZoneProblem(ctx, "Zone deletion body is not allowed")
		return
	}
	next(ctx)
}

func (s *Server) writeZoneProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Zone request problem", slog.Any("error", err))
	}
}
