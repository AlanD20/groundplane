package handlers

import (
	"context"
	"net/http"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type taskAttemptEventsInput struct {
	ID        string       `path:"id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	WorkID    string       `path:"work_id"`
	AttemptID string       `path:"attempt_id"`
	After     taskSequence `query:"after"`
	Revision  int64        `query:"revision" minimum:"0"`
}

type taskAttemptEventsOutput struct{ Body apiTypes.TaskAttemptEvents }

func (s *Server) registerTaskAttemptEvents() {
	huma.Register(s.API, huma.Operation{OperationID: "task.attempt-events", Method: http.MethodGet,
		Path: "/tasks/{id}/work/{work_id}/attempts/{attempt_id}/events", Summary: "Inspect retained execution events for one attempt",
		Tags: []string{"Task"}}, s.taskAttemptEvents)
}

func (s *Server) taskAttemptEvents(
	ctx context.Context,
	request *taskAttemptEventsInput,
) (*taskAttemptEventsOutput, error) {
	if s.taskInspection == nil {
		return nil, errs.New(errs.KindInternal, "task inspection is not configured")
	}
	value, err := s.taskInspection.Events(
		ctx,
		request.ID,
		request.WorkID,
		request.AttemptID,
		uint64(request.After),
		request.Revision,
	)
	if err != nil {
		return nil, normalizeTaskRouteError(err)
	}
	body := apiTypes.TaskAttemptEvents{Items: []apiTypes.TaskAttemptEvent{}, First: value.First, Last: value.Last,
		Expired: value.Expired, Incomplete: value.Incomplete, Revision: value.Revision, FinishedAt: value.FinishedAt, Outcome: value.Outcome}
	for _, event := range value.Items {
		body.Items = append(
			body.Items,
			apiTypes.TaskAttemptEvent{Sequence: event.Sequence, RecordedAt: event.RecordedAt, Stage: event.Stage},
		)
	}
	return &taskAttemptEventsOutput{Body: body}, nil
}
