package controller

import (
	"context"
	"log/slog"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type attachListInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit       int    `query:"limit" required:"false"`
	Cursor      string `query:"cursor" required:"false"`
}

type attachPageOutput struct {
	Body apiTypes.Page[apiTypes.Attach]
}

func (s *Server) registerAttachList() {
	huma.Register(s.API, huma.Operation{
		OperationID: "attach.list", Method: "GET", Path: "/attaches",
		Summary: "List attaches", Tags: []string{"Attach"},
		Middlewares: huma.Middlewares{s.validateAttachListQuery},
	}, s.listAttaches)
}

func (s *Server) listAttaches(
	ctx context.Context,
	request *attachListInput,
) (*attachPageOutput, error) {
	if s.attachMutations == nil {
		return nil, errs.New(errs.KindInternal, "Attach service is not configured")
	}
	pageRequest, err := attachListRequest(request.Environment, request.Limit, request.Cursor)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	page, err := s.attachMutations.ListAttaches(ctx, request.Environment, pageRequest)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.Page[apiTypes.Attach]{
		Items: make([]apiTypes.Attach, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = attachResponse(item.Record)
	}
	return &attachPageOutput{Body: response}, nil
}

func attachListRequest(environmentID string, limit int, cursor string) (etcd.PageRequest, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Attach list requires a stable Environment id",
		)
	}
	if limit < 0 {
		return etcd.PageRequest{}, errs.New(errs.KindValidationFailed, "Attach list limit must be a positive integer")
	}
	return etcd.PageRequest{Limit: limit, Cursor: cursor}, nil
}

func attachResponse(record etcd.AttachRecord) apiTypes.Attach {
	return apiTypes.Attach{
		ID: record.ID, Name: record.Name,
		ServiceIDs: append([]string(nil), record.ServiceIDs...), BackingServiceID: record.BackingServiceID,
		BackingEnvironmentID: record.BackingEnvironmentID, BackingNetworkID: record.BackingNetworkID,
		GrantAttachIDs: append([]string(nil), record.GrantAttachIDs...), Status: string(record.Status),
	}
}

func (s *Server) validateAttachListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	for key, values := range requestURL.Query() {
		if key != "environment" && key != "limit" && key != "cursor" {
			s.writeAttachListProblem(ctx, "Attach list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeAttachListProblem(ctx, "Attach list query contains duplicate values")
			return
		}
	}
	next(ctx)
}

func (s *Server) writeAttachListProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, 400, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Attach list request problem", slog.Any("error", err))
	}
}
