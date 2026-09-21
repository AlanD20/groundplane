package handlers

import (
	"context"
	entrycapability "github.com/AlanD20/groundplane/internal/controller/entry"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (s *Server) listEntries(ctx context.Context, request *entryListInput) (*entryPageOutput, error) {
	if s.entries == nil {
		return nil, errs.New(errs.KindInternal, "entry reader is not configured")
	}
	page, err := s.entries.ListEntries(ctx, request.Environment, etcdstore.PageRequest{
		Limit: request.Limit, Cursor: request.Cursor,
	})
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.Page[apiTypes.Entry]{
		Items: make([]apiTypes.Entry, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = entrycapability.Response(item.Record)
	}
	return &entryPageOutput{Body: response}, nil
}

func (s *Server) showEntry(ctx context.Context, request *entryShowInput) (*entryOutput, error) {
	if s.entries == nil {
		return nil, errs.New(errs.KindInternal, "entry reader is not configured")
	}
	stored, err := s.entries.GetEntry(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &entryOutput{Body: entrycapability.Response(stored.Record)}, nil
}

func (s *Server) revealEntry(ctx context.Context, request *entryShowInput) (*entryValueOutput, error) {
	if s.entries == nil {
		return nil, errs.New(errs.KindInternal, "entry reader is not configured")
	}
	value, err := s.entries.RevealEntry(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &entryValueOutput{Body: apiTypes.EntryValue{Value: value}}, nil
}
