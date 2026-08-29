package controller

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type BackupKeyMutator interface {
	RotateBackupKey(context.Context, string, string) (etcd.IdempotencyResponse, error)
}

type BackupKeyExporter interface {
	ExportBackupKey(context.Context, string) (backupkey.Export, error)
}

type backupKeyRotateInput struct {
	ID  string `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type backupKeyExportInput struct {
	ID string `path:"id" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type backupKeyMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

type backupKeyExportOutput struct {
	ContentType        string `header:"Content-Type"`
	ContentDisposition string `header:"Content-Disposition"`
	CacheControl       string `header:"Cache-Control"`
	Body               func(huma.Context)
}

func (s *Server) registerBackupKeyRoutes() {
	acceptedSchema := openAPISchema[apiTypes.TaskAccepted](s.API.OpenAPI().Components.Schemas, "TaskAccepted")
	huma.Register(s.API, huma.Operation{
		OperationID: "backup.key.rotate", Method: http.MethodPost, Path: "/environments/{id}/rotate-key",
		Summary: "Rotate the Environment backup age key", Tags: []string{"Backup"}, DefaultStatus: http.StatusAccepted,
		Middlewares: huma.Middlewares{s.rejectTaskMutationBody, s.rejectTaskMutationQuery},
		Responses:   attachMutationResponses(acceptedSchema),
	}, s.rotateBackupKey)
	huma.Register(s.API, huma.Operation{
		OperationID: "backup.key.export", Method: http.MethodPost, Path: "/environments/{id}/export-key",
		Summary: "Export the current backup age identity without storing it", Tags: []string{"Backup"}, DefaultStatus: http.StatusOK,
		Middlewares: huma.Middlewares{s.rejectTaskMutationBody, s.rejectTaskMutationQuery},
		Responses: map[string]*huma.Response{"200": {
			Description: http.StatusText(http.StatusOK), Content: map[string]*huma.MediaType{"text/plain": {Schema: &huma.Schema{Type: huma.TypeString}}},
		}},
	}, s.exportBackupKey)
	s.setRoutePolicy("POST /api/v1/environments/{id}/rotate-key", routePolicy{body: bodyless})
	s.setRoutePolicy("POST /api/v1/environments/{id}/export-key", routePolicy{body: bodyless})
}

func (s *Server) rotateBackupKey(ctx context.Context, request *backupKeyRotateInput) (*backupKeyMutationOutput, error) {
	if s.backupKeyMutations == nil {
		return nil, errs.New(errs.KindInternal, "backup key mutator is not configured")
	}
	response, err := s.backupKeyMutations.RotateBackupKey(ctx, request.ID, request.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	body := append([]byte(nil), response.Body...)
	clear(response.Body)
	return &backupKeyMutationOutput{Status: response.Status, ContentType: response.ContentKind, Body: func(ctx huma.Context) {
		defer clear(body)
		ctx.SetStatus(response.Status)
		if _, writeErr := ctx.BodyWriter().Write(body); writeErr != nil && s.Logger != nil {
			s.Logger.Error("controller: write Backup key rotation response", slog.Any("error", writeErr))
		}
	}}, nil
}

func (s *Server) exportBackupKey(ctx context.Context, request *backupKeyExportInput) (*backupKeyExportOutput, error) {
	if s.backupKeyExports == nil {
		return nil, errs.New(errs.KindInternal, "backup key exporter is not configured")
	}
	result, err := s.backupKeyExports.ExportBackupKey(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	defer clear(result.Identity)
	identity := append([]byte(nil), result.Identity...)
	return &backupKeyExportOutput{
		ContentType:        "text/plain; charset=utf-8",
		ContentDisposition: "attachment; filename=\"groundplane-" + request.ID + "-age-era-" + formatEra(result.Era) + "-identity.txt\"",
		CacheControl:       "no-store",
		Body: func(ctx huma.Context) {
			defer clear(identity)
			ctx.SetStatus(http.StatusOK)
			if _, writeErr := ctx.BodyWriter().Write(identity); writeErr != nil && s.Logger != nil {
				s.Logger.Error("controller: write Backup key export response", slog.Any("error", writeErr))
			}
		},
	}, nil
}

func formatEra(era int) string {
	if era < 1 {
		return "0"
	}
	return strconv.Itoa(era)
}
