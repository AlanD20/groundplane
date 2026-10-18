package handlers

import (
	"context"
	"net/http"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type taskAttemptsInput struct {
	ID       string `path:"id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	WorkID   string `path:"work_id"`
	After    uint64 `query:"after"`
	Limit    int64  `query:"limit" default:"25" minimum:"1" maximum:"100"`
	Revision int64  `query:"revision" minimum:"0"`
}

type taskAttemptsOutput struct{ Body apiTypes.TaskAttemptPage }

type taskImageInput struct {
	ID       string `path:"id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	WorkID   string `path:"work_id"`
	Revision int64  `query:"revision" minimum:"0"`
}

type taskImageOutput struct{ Body apiTypes.TaskImageExecution }

type taskStagesInput struct {
	ID        string `path:"id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	WorkID    string `path:"work_id"`
	AttemptID string `path:"attempt_id"`
	Revision  int64  `query:"revision" minimum:"0"`
}

type taskStagesOutput struct{ Body apiTypes.TaskStages }

func (s *Server) registerTaskWork() {
	s.registerTaskAttemptEvents()
	huma.Register(s.API, huma.Operation{OperationID: "task.stages", Method: http.MethodGet,
		Path: "/tasks/{id}/work/{work_id}/attempts/{attempt_id}/stages", Summary: "Inspect named execution stages and their observed results",
		Tags: []string{"Task"}}, s.taskStages)
	huma.Register(s.API, huma.Operation{OperationID: "task.image", Method: http.MethodGet,
		Path: "/tasks/{id}/work/{work_id}/image", Summary: "Inspect image operation evidence and download progress",
		Tags: []string{"Task"}}, s.taskImage)
	huma.Register(s.API, huma.Operation{OperationID: "task.attempts", Method: http.MethodGet,
		Path: "/tasks/{id}/work/{work_id}/attempts", Summary: "Inspect execution attempts for Task work",
		Tags: []string{"Task"}}, s.taskAttempts)
}

func (s *Server) taskStages(ctx context.Context, request *taskStagesInput) (*taskStagesOutput, error) {
	if s.taskInspection == nil {
		return nil, errs.New(errs.KindInternal, "task inspection is not configured")
	}
	value, err := s.taskInspection.Stages(ctx, request.ID, request.WorkID, request.AttemptID, request.Revision)
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	return &taskStagesOutput{Body: apiTypes.TaskStages{Items: value.Items, Available: value.Available, Sequence: value.Sequence, UpdatedAt: value.UpdatedAt, Revision: value.Revision, Incomplete: value.Incomplete}}, nil
}

func (s *Server) taskImage(ctx context.Context, request *taskImageInput) (*taskImageOutput, error) {
	if s.taskInspection == nil {
		return nil, errs.New(errs.KindInternal, "task inspection is not configured")
	}
	value, err := s.taskInspection.Image(ctx, request.ID, request.WorkID, request.Revision)
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	return &taskImageOutput{Body: apiTypes.TaskImageExecution{TaskID: value.TaskID, WorkItemID: value.WorkItemID,
		AttemptID: value.AttemptID, Requested: value.Requested, Manifest: value.Manifest, ImageID: value.ImageID,
		Platform: value.Platform, Phase: string(value.Phase), UncertainPhase: string(value.UncertainPhase),
		Outcome: value.Outcome, Summary: value.Summary, Progress: value.Progress,
		ProgressUnavailable: value.ProgressUnavailable, UpdatedAt: value.UpdatedAt,
		FinishedAt: value.FinishedAt, Revision: value.Revision}}, nil
}

func (s *Server) taskAttempts(ctx context.Context, request *taskAttemptsInput) (*taskAttemptsOutput, error) {
	if s.taskInspection == nil {
		return nil, errs.New(errs.KindInternal, "task inspection is not configured")
	}
	page, err := s.taskInspection.Attempts(ctx, request.ID, request.WorkID, request.After, request.Limit, request.Revision)
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	response := apiTypes.TaskAttemptPage{Items: []apiTypes.TaskAttempt{}, More: page.More, After: page.After, Revision: page.Revision}
	for _, attempt := range page.Attempts {
		response.Items = append(response.Items, executionAttempt(attempt))
	}
	return &taskAttemptsOutput{Body: response}, nil
}
