package controller

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type RunnerReader interface {
	GetRunner(context.Context, string) (etcd.Versioned[etcd.RunnerRecord], error)
	ListRunners(context.Context, etcd.RunnerFilter, etcd.PageRequest) (etcd.Page[etcd.RunnerRecord], error)
	GetRunnerObservation(context.Context, string) (etcd.Versioned[etcd.RunnerObservationRecord], bool, error)
}

type runnerListInput struct {
	Tenant  string `query:"tenant" pattern:"^tnt_[0-9A-HJKMNP-TV-Z]{26}$"`
	Project string `query:"project" pattern:"^prj_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit   int    `query:"limit" required:"false" minimum:"1" maximum:"200"`
	Cursor  string `query:"cursor" required:"false"`
}

type runnerShowInput struct {
	ID string `path:"id" pattern:"^run_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type runnerPageOutput struct {
	Body apiTypes.Page[apiTypes.Runner]
}

type runnerOutput struct {
	Body apiTypes.Runner
}

func (s *Server) registerRunners() {
	huma.Register(s.API, huma.Operation{
		OperationID: "runner.list", Method: http.MethodGet, Path: "/runners",
		Summary: "List managed runners", Tags: []string{"Runner"},
	}, s.listRunners)
	huma.Register(s.API, huma.Operation{
		OperationID: "runner.show", Method: http.MethodGet, Path: "/runners/{id}",
		Summary: "Show a managed runner", Tags: []string{"Runner"},
	}, s.showRunner)
}

func (s *Server) listRunners(
	ctx context.Context,
	request *runnerListInput,
) (*runnerPageOutput, error) {
	if s.runners == nil {
		return nil, errs.New(errs.KindInternal, "Runner reader is not configured")
	}
	if (request.Tenant == "") == (request.Project == "") {
		return nil, errs.New(errs.KindValidationFailed, "Runner list requires exactly one Tenant or Project")
	}
	page, err := s.runners.ListRunners(ctx, etcd.RunnerFilter{
		TenantID: request.Tenant, ProjectID: request.Project,
	}, etcd.PageRequest{Limit: request.Limit, Cursor: request.Cursor})
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	items := make([]apiTypes.Runner, len(page.Items))
	for index, item := range page.Items {
		online, err := s.runnerOnline(ctx, item.Record.Desired.ID)
		if err != nil {
			return nil, normalizeProjectError(err)
		}
		items[index] = runnerResponse(item.Record, online)
	}
	return &runnerPageOutput{Body: apiTypes.Page[apiTypes.Runner]{
		Items: items, NextCursor: page.NextCursor,
	}}, nil
}

func (s *Server) showRunner(
	ctx context.Context,
	request *runnerShowInput,
) (*runnerOutput, error) {
	if s.runners == nil {
		return nil, errs.New(errs.KindInternal, "Runner reader is not configured")
	}
	runner, err := s.runners.GetRunner(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	online, err := s.runnerOnline(ctx, runner.Record.Desired.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &runnerOutput{Body: runnerResponse(runner.Record, online)}, nil
}

func (s *Server) runnerOnline(ctx context.Context, runnerID string) (bool, error) {
	observation, found, err := s.runners.GetRunnerObservation(ctx, runnerID)
	if err != nil || !found {
		return false, err
	}
	return observation.Record.Online, nil
}

func runnerResponse(record etcd.RunnerRecord, online bool) apiTypes.Runner {
	projectID := ""
	if record.Desired.OwnerKind == etcd.RunnerOwnerProject {
		projectID = record.Desired.OwnerID
	}
	return apiTypes.Runner{
		ID: record.Desired.ID, TenantID: record.Desired.TenantID, ProjectID: projectID,
		Labels: append([]string(nil), record.Desired.Labels...), Online: online,
	}
}
