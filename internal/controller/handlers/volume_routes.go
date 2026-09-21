package handlers

import (
	"context"
	"errors"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"io"
	"log/slog"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

// VolumeReader is the storage-backed collection seam. Runtime fields are
// supplied by VolumeViewReader when the app composition root has the runtime
// projection available; the collection remains independently useful for
// bootstrap and focused read tests.
type VolumeReader interface {
	ListVolumes(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[etcd.VolumeRecord], error)
}

type volumeViewReader interface {
	ListVolumeViews(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[apiTypes.Volume], error)
}

type volumeDetailReader interface {
	GetVolume(context.Context, string) (apiTypes.Volume, error)
}

type volumeImpactReader interface {
	GetVolumeDeletionImpact(context.Context, string, string, int) (apiTypes.VolumeDeletionImpactPage, error)
}

// VolumeMutator is the public Controller boundary for the three asynchronous
// identity operations. The app owns publication and replay; the HTTP adapter
// preserves its exact response bytes.
type VolumeMutator interface {
	CreateVolume(context.Context, apiTypes.VolumeCreate, string) (idempotencyrecord.IdempotencyResponse, error)
	EditVolume(context.Context, string, apiTypes.VolumeEdit, string) (idempotencyrecord.IdempotencyResponse, error)
	RemoveVolume(context.Context, string, string, string, string) (idempotencyrecord.IdempotencyResponse, error)
}

type volumeListInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit       int    `query:"limit" required:"false"`
	Cursor      string `query:"cursor" required:"false"`
}

type volumeShowInput struct {
	ID string `path:"id" pattern:"^vol_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type volumeCreateInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.VolumeCreate
}

type volumeEditInput struct {
	ID             string `path:"id" pattern:"^vol_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.VolumeEdit
}

type volumeImpactInput struct {
	ID     string `path:"id" pattern:"^vol_[0-9A-HJKMNP-TV-Z]{26}$"`
	Cursor string `query:"cursor" required:"false" maxLength:"4096"`
	Limit  int    `query:"limit" required:"false" minimum:"1" maximum:"40"`
}

type volumeRemoveInput struct {
	ID             string `path:"id" pattern:"^vol_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	ImpactToken    string `query:"impact_token" required:"true" pattern:"^[a-f0-9]{64}$"`
	ConfirmKey     string `query:"confirm_key" required:"true" minLength:"1" maxLength:"255"`
}

type volumePageOutput struct {
	Body apiTypes.Page[apiTypes.Volume]
}

type volumeOutput struct {
	Body apiTypes.Volume
}

type volumeImpactOutput struct {
	Body apiTypes.VolumeDeletionImpactPage
}

type volumeMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerVolumes() {
	mutationSchema := volumeMutationResponseSchema()
	taskAcceptedSchema := taskAcceptedResponseSchema()

	huma.Register(s.API, huma.Operation{
		OperationID: "volume.list", Method: http.MethodGet, Path: "/volumes",
		Summary: "List Environment volumes", Tags: []string{"Volume"},
	}, s.listVolumes)
	huma.Register(s.API, huma.Operation{
		OperationID: "volume.show", Method: http.MethodGet, Path: "/volumes/{id}",
		Summary: "Show an Environment volume", Tags: []string{"Volume"},
	}, s.showVolume)
	huma.Register(s.API, huma.Operation{
		OperationID: "volume.create", Method: http.MethodPost, Path: "/volumes",
		Summary: "Add an Environment volume", Tags: []string{"Volume"}, DefaultStatus: http.StatusCreated,
		Responses: map[string]*huma.Response{
			"201": {Description: http.StatusText(http.StatusCreated), Content: map[string]*huma.MediaType{
				"application/json": {Schema: mutationSchema},
			}},
		},
	}, s.createVolume)
	huma.Register(s.API, huma.Operation{
		OperationID: "volume.edit", Method: http.MethodPatch, Path: "/volumes/{id}",
		Summary: "Edit a volume slug", Tags: []string{"Volume"}, DefaultStatus: http.StatusOK,
		Responses: map[string]*huma.Response{
			"200": {Description: http.StatusText(http.StatusOK), Content: map[string]*huma.MediaType{
				"application/json": {Schema: mutationSchema},
			}},
		},
	}, s.editVolume)
	huma.Register(s.API, huma.Operation{
		OperationID: "volume.removal-impact", Method: http.MethodGet, Path: "/volumes/{id}/deletion-impact",
		Summary: "Preview the fixed-revision impact of removing a volume", Tags: []string{"Volume"},
	}, s.volumeDeletionImpact)
	huma.Register(s.API, huma.Operation{
		OperationID: "volume.remove", Method: http.MethodDelete, Path: "/volumes/{id}",
		Summary: "Remove an Environment volume", Tags: []string{"Volume"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectVolumeDeleteBody, s.validateVolumeDeleteQuery},
		Responses:   attachMutationResponses(taskAcceptedSchema),
	}, s.removeVolume)
	s.setRoutePolicy("POST /api/v1/volumes", routePolicy{body: jsonBody})
	s.setRoutePolicy("PATCH /api/v1/volumes/{id}", routePolicy{body: jsonBody})
}

func volumeMutationResponseSchema() *huma.Schema {
	return &huma.Schema{
		Type: "object",
		Properties: map[string]*huma.Schema{
			"volume":  {Ref: "#/components/schemas/Volume"},
			"task_id": {Type: "string"},
		},
		Required:             []string{"volume", "task_id"},
		AdditionalProperties: false,
	}
}

func taskAcceptedResponseSchema() *huma.Schema {
	return &huma.Schema{
		Type: "object",
		Properties: map[string]*huma.Schema{
			"task_id": {Type: "string"},
		},
		Required:             []string{"task_id"},
		AdditionalProperties: false,
	}
}

func (s *Server) listVolumes(ctx context.Context, request *volumeListInput) (*volumePageOutput, error) {
	if s.volumes == nil {
		return nil, errs.New(errs.KindInternal, "volume reader is not configured")
	}
	pageRequest, err := volumeListRequest(request.Environment, request.Limit, request.Cursor)
	if err != nil {
		return nil, err
	}
	if reader, ok := s.volumes.(volumeViewReader); ok {
		page, err := reader.ListVolumeViews(ctx, request.Environment, pageRequest)
		if err != nil {
			return nil, normalizeProjectError(err)
		}
		items := make([]apiTypes.Volume, len(page.Items))
		for index, item := range page.Items {
			items[index] = item.Record
		}
		return &volumePageOutput{Body: apiTypes.Page[apiTypes.Volume]{Items: items, NextCursor: page.NextCursor}}, nil
	}
	page, err := s.volumes.ListVolumes(ctx, request.Environment, pageRequest)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	items := make([]apiTypes.Volume, len(page.Items))
	for index, item := range page.Items {
		items[index] = volumeRecordResponse(item.Record)
	}
	return &volumePageOutput{Body: apiTypes.Page[apiTypes.Volume]{Items: items, NextCursor: page.NextCursor}}, nil
}

func (s *Server) showVolume(ctx context.Context, request *volumeShowInput) (*volumeOutput, error) {
	reader, ok := s.volumes.(volumeDetailReader)
	if !ok || reader == nil {
		return nil, errs.New(errs.KindInternal, "volume detail reader is not configured")
	}
	volume, err := reader.GetVolume(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &volumeOutput{Body: volume}, nil
}

func (s *Server) createVolume(ctx context.Context, request *volumeCreateInput) (*volumeMutationOutput, error) {
	if s.volumeMutations == nil {
		return nil, errs.New(errs.KindInternal, "volume mutator is not configured")
	}
	response, err := s.volumeMutations.CreateVolume(ctx, request.Body, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.volumeMutationResponse(response, "create"), nil
}

func (s *Server) editVolume(ctx context.Context, request *volumeEditInput) (*volumeMutationOutput, error) {
	if s.volumeMutations == nil {
		return nil, errs.New(errs.KindInternal, "volume mutator is not configured")
	}
	response, err := s.volumeMutations.EditVolume(ctx, request.ID, request.Body, request.IdempotencyKey)
	if err != nil {
		normalized := normalizeProjectError(err)
		if kind, ok := errs.KindOf(normalized); ok && kind == errs.KindInternal && s.Logger != nil {
			s.Logger.Error("controller: edit Volume", slog.String("volume_id", request.ID), slog.Any("error", err))
		}
		return nil, normalized
	}
	return s.volumeMutationResponse(response, "edit"), nil
}

func (s *Server) volumeDeletionImpact(ctx context.Context, request *volumeImpactInput) (*volumeImpactOutput, error) {
	reader, ok := s.volumes.(volumeImpactReader)
	if !ok || reader == nil {
		return nil, errs.New(errs.KindInternal, "volume deletion-impact reader is not configured")
	}
	limit := request.Limit
	if limit == 0 {
		limit = 40
	}
	if limit < 1 || limit > 40 {
		return nil, errs.New(errs.KindValidationFailed, "volume deletion-impact limit must be between 1 and 40")
	}
	page, err := reader.GetVolumeDeletionImpact(ctx, request.ID, request.Cursor, limit)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &volumeImpactOutput{Body: page}, nil
}

func (s *Server) removeVolume(ctx context.Context, request *volumeRemoveInput) (*volumeMutationOutput, error) {
	if s.volumeMutations == nil {
		return nil, errs.New(errs.KindInternal, "volume mutator is not configured")
	}
	response, err := s.volumeMutations.RemoveVolume(
		ctx, request.ID, request.ImpactToken, request.ConfirmKey, request.IdempotencyKey,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return s.volumeMutationResponse(response, "remove"), nil
}

func (s *Server) volumeMutationResponse(
	response idempotencyrecord.IdempotencyResponse,
	action string,
) *volumeMutationOutput {
	return &volumeMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
				s.Logger.Error(
					"controller: write Volume mutation response",
					slog.String("action", action),
					slog.Any("error", err),
				)
			}
		},
	}
}

func (s *Server) rejectVolumeDeleteBody(ctx huma.Context, next func(huma.Context)) {
	var probe [1]byte
	count, err := ctx.BodyReader().Read(probe[:])
	if count != 0 || (err != nil && !errors.Is(err, io.EOF)) {
		if writeErr := huma.WriteErr(s.API, ctx, http.StatusBadRequest, "Volume deletion body is not allowed"); writeErr != nil &&
			s.Logger != nil {
			s.Logger.Error("controller: write Volume deletion problem", slog.Any("error", writeErr))
		}
		return
	}
	next(ctx)
}

func (s *Server) validateVolumeDeleteQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	for key, values := range requestURL.Query() {
		if (key != "impact_token" && key != "confirm_key") || len(values) != 1 {
			if writeErr := huma.WriteErr(s.API, ctx, http.StatusBadRequest, "Volume deletion query is invalid"); writeErr != nil &&
				s.Logger != nil {
				s.Logger.Error("controller: write Volume deletion problem", slog.Any("error", writeErr))
			}
			return
		}
	}
	next(ctx)
}

func volumeRecordResponse(record etcd.VolumeRecord) apiTypes.Volume {
	return apiTypes.Volume{ID: record.ID, EnvironmentID: record.EnvironmentID, Slug: record.Slug, Key: record.Key}
}

func volumeListRequest(environmentID string, limit int, cursor string) (etcdstore.PageRequest, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcdstore.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Volume list requires a stable Environment id",
		)
	}
	if limit < 0 {
		return etcdstore.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Volume list limit must be a positive integer",
		)
	}
	return etcdstore.PageRequest{Limit: limit, Cursor: cursor}, nil
}
