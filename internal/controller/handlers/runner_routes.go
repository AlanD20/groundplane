package handlers

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"net/http"

	runnercapability "github.com/AlanD20/groundplane/internal/controller/runner"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type RunnerReader interface {
	GetRunner(context.Context, string) (etcdstore.Versioned[runnerrecord.RunnerRecord], error)
	ListRunners(context.Context, runnerrecord.RunnerFilter, etcdstore.PageRequest) (etcdstore.Page[runnerrecord.RunnerRecord], error)
	GetRunnerObservation(context.Context, string) (etcdstore.Versioned[runnerrecord.RunnerObservationRecord], bool, error)
	GetRunnerDeletionTombstone(context.Context, string) (etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord], bool, error)
}

type RunnerMutator interface {
	RenameRunner(context.Context, string, apiTypes.RunnerEditRequest, string) (idempotencyrecord.IdempotencyResponse, error)
}

type RunnerRemover interface {
	RemoveRunner(context.Context, string, string) (idempotencyrecord.IdempotencyResponse, error)
}

type RunnerProvisioner interface {
	CreateRunner(context.Context, apiTypes.RunnerCreateRequest, string) (idempotencyrecord.IdempotencyResponse, error)
	RetryRunner(context.Context, string, apiTypes.RunnerRetryRequest, string) (idempotencyrecord.IdempotencyResponse, error)
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
	s.registerRunnerProvisioning()
	s.registerRunnerEdit()
	s.registerRunnerRemove()
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
	page, err := s.runners.ListRunners(ctx, runnerrecord.RunnerFilter{
		TenantID: request.Tenant, ProjectID: request.Project,
	}, etcdstore.PageRequest{Limit: request.Limit, Cursor: request.Cursor})
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	items := make([]apiTypes.Runner, len(page.Items))
	for index, item := range page.Items {
		projection, err := s.runnerProjection(ctx, item.Record)
		if err != nil {
			return nil, normalizeProjectError(err)
		}
		items[index] = projection
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
	projection, err := s.runnerProjection(ctx, runner.Record)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &runnerOutput{Body: projection}, nil
}

func (s *Server) runnerProjection(ctx context.Context, record runnerrecord.RunnerRecord) (apiTypes.Runner, error) {
	observation, observed, err := s.runners.GetRunnerObservation(ctx, record.Desired.ID)
	if err != nil {
		return apiTypes.Runner{}, err
	}
	tombstone, deleting, err := s.runners.GetRunnerDeletionTombstone(ctx, record.Desired.ID)
	if err != nil {
		return apiTypes.Runner{}, err
	}
	var observationRecord *runnerrecord.RunnerObservationRecord
	if observed {
		observationRecord = &observation.Record
	}
	var tombstoneRecord *deletionrecord.DeletionTombstoneRecord
	if deleting {
		tombstoneRecord = &tombstone.Record
	}
	return runnercapability.PublicProjection(record, observationRecord, tombstoneRecord)
}
