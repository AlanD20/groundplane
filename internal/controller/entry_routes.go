package controller

import (
	"context"
	"log/slog"
	"net/http"

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

func (s *Server) registerEntries() {
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
		value := *entry.UID
		response.UID = &value
	}
	if entry.GID != nil {
		value := *entry.GID
		response.GID = &value
	}
	if entry.Source.Fact != nil {
		response.Source.AttachID = entry.Source.Fact.Attach
		response.Source.GrantAttachID = entry.Source.Fact.Grant
		response.Source.Fact = entry.Source.Fact.Key
	}
	return response
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
