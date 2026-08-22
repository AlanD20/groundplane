package controller

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type SecretReader interface {
	GetSecret(context.Context, string) (etcd.Versioned[etcd.SecretRecord], error)
	ListSecrets(context.Context, core.SecretScope, string, etcd.PageRequest) (etcd.Page[etcd.SecretRecord], error)
	RevealSecret(context.Context, string) (string, error)
}

type secretListInput struct {
	Project  string `query:"project" required:"false" pattern:"^prj_[0-9A-HJKMNP-TV-Z]{26}$"`
	Platform bool   `query:"platform" required:"false"`
	Limit    int    `query:"limit" required:"false"`
	Cursor   string `query:"cursor" required:"false"`
}

type secretShowInput struct {
	ID string `path:"id" pattern:"^sec_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type secretPageOutput struct {
	Body apiTypes.Page[apiTypes.Secret]
}

type secretOutput struct {
	Body apiTypes.Secret
}

type secretValueOutput struct {
	Body apiTypes.SecretValue
}

func (s *Server) registerSecrets() {
	huma.Register(s.API, huma.Operation{
		OperationID: "secret.list", Method: http.MethodGet, Path: "/secrets",
		Summary: "List reusable secrets", Tags: []string{"Secret"},
		Middlewares: huma.Middlewares{s.validateSecretListQuery},
	}, s.listSecrets)
	huma.Register(s.API, huma.Operation{
		OperationID: "secret.show", Method: http.MethodGet, Path: "/secrets/{id}",
		Summary: "Show reusable secret metadata", Tags: []string{"Secret"},
	}, s.showSecret)
	huma.Register(s.API, huma.Operation{
		OperationID: "secret.reveal", Method: http.MethodGet, Path: "/secrets/{id}/value",
		Summary: "Reveal a reusable secret value", Tags: []string{"Secret"},
	}, s.revealSecret)
}

func (s *Server) listSecrets(ctx context.Context, request *secretListInput) (*secretPageOutput, error) {
	if s.secrets == nil {
		return nil, errs.New(errs.KindInternal, "Secret reader is not configured")
	}
	scope, projectID, pageRequest, err := secretListRequest(
		request.Project,
		request.Platform,
		request.Limit,
		request.Cursor,
	)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	page, err := s.secrets.ListSecrets(ctx, scope, projectID, pageRequest)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.Page[apiTypes.Secret]{
		Items: make([]apiTypes.Secret, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = secretResponse(item.Record)
	}
	return &secretPageOutput{Body: response}, nil
}

func (s *Server) showSecret(ctx context.Context, request *secretShowInput) (*secretOutput, error) {
	if s.secrets == nil {
		return nil, errs.New(errs.KindInternal, "Secret reader is not configured")
	}
	stored, err := s.secrets.GetSecret(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &secretOutput{Body: secretResponse(stored.Record)}, nil
}

func (s *Server) revealSecret(ctx context.Context, request *secretShowInput) (*secretValueOutput, error) {
	if s.secrets == nil {
		return nil, errs.New(errs.KindInternal, "Secret reader is not configured")
	}
	value, err := s.secrets.RevealSecret(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &secretValueOutput{Body: apiTypes.SecretValue{Value: value}}, nil
}

func secretListRequest(
	projectID string,
	platform bool,
	limit int,
	cursor string,
) (core.SecretScope, string, etcd.PageRequest, error) {
	if projectID != "" && platform || projectID == "" && !platform {
		return "", "", etcd.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Secret list requires exactly one owner selector",
		)
	}
	if projectID != "" && ids.Validate(ids.KindProject, projectID) != nil {
		return "", "", etcd.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Secret list requires a stable Project id",
		)
	}
	if limit < 0 {
		return "", "", etcd.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Secret list limit must be a positive integer",
		)
	}
	if platform {
		return core.SecretScopePlatform, "", etcd.PageRequest{Limit: limit, Cursor: cursor}, nil
	}
	return core.SecretScopeProject, projectID, etcd.PageRequest{Limit: limit, Cursor: cursor}, nil
}

func secretResponse(record etcd.SecretRecord) apiTypes.Secret {
	secret := record.Secret
	return apiTypes.Secret{
		ID: secret.ID, Scope: string(secret.Scope), ProjectID: secret.ProjectID,
		Key: secret.Key, Kind: string(secret.Kind), Ref: secret.Ref,
		UpdatedAt: secret.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func (s *Server) validateSecretListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	query := requestURL.Query()
	for key, values := range query {
		if key != "project" && key != "platform" && key != "limit" && key != "cursor" {
			s.writeSecretProblem(ctx, "Secret list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeSecretProblem(ctx, "Secret list query contains duplicate values")
			return
		}
	}
	projectValues, hasProject := query["project"]
	platformValues, hasPlatform := query["platform"]
	if hasProject == hasPlatform || hasProject && projectValues[0] == "" ||
		hasPlatform && platformValues[0] != "true" {
		s.writeSecretProblem(ctx, "Secret list requires exactly one owner selector")
		return
	}
	next(ctx)
}

func (s *Server) writeSecretProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Secret request problem", slog.Any("error", err))
	}
}
