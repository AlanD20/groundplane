package handlers

import (
	"context"
	"encoding/json"
	"errors"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
)

const (
	taskIDPattern          = "^task_[0-9A-HJKMNP-TV-Z]{26}$"
	lastEventIDPattern     = "^(0|[1-9][0-9]{0,19})$"
	positiveEventIDPattern = "^[1-9][0-9]{0,19}$"
)

type taskEventRunner interface {
	Run(context.Context, func(taskjournal.TaskEventRecord) error) error
}

type taskEventStreamOpener interface {
	OpenTaskEventStream(context.Context, string, uint64) (taskEventRunner, error)
}

type taskEventStreamInput struct {
	ID          string `path:"id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	LastEventID string `header:"Last-Event-ID" required:"false" pattern:"^(0|[1-9][0-9]{0,19})$" doc:"Canonical decimal Task-event sequence; absent or 0 replays the retained journal. Trimmed resume cursors expire."`
}

type repositoryTaskEventStreamOpener struct {
	repository *etcd.TaskRepository
}

func (opener repositoryTaskEventStreamOpener) OpenTaskEventStream(
	ctx context.Context,
	taskID string,
	after uint64,
) (taskEventRunner, error) {
	return opener.repository.OpenTaskEventStream(ctx, taskID, after)
}

func (s *Server) registerTaskEventStream() {
	s.API.OpenAPI().Components.Schemas.Map()["TaskEvent"] = taskEventOpenAPISchema()
	huma.Register(s.API, huma.Operation{
		OperationID: "task.events", Method: http.MethodGet, Path: "/tasks/{id}/events",
		Summary:     "Stream durable Task events",
		Description: "Replays and follows Task events using sequence-based Last-Event-ID resume.",
		Tags:        []string{"Task"}, DefaultStatus: http.StatusOK, SkipValidateParams: true,
		Middlewares: huma.Middlewares{s.validateTaskEventStreamRequest},
		Responses: map[string]*huma.Response{
			"200": {
				Description: "Durable Task event stream",
				Content: map[string]*huma.MediaType{
					"text/event-stream": {Schema: taskEventStreamOpenAPISchema()},
				},
			},
			"default": {
				Description: "Error",
				Content: map[string]*huma.MediaType{
					"application/problem+json": {Schema: &huma.Schema{Ref: "#/components/schemas/Error"}},
				},
			},
		},
	}, s.taskEventStream)
	s.setRoutePolicy("GET /api/v1/tasks/{id}/events", routePolicy{streaming: true})
}

func (s *Server) validateTaskEventStreamRequest(ctx huma.Context, next func(huma.Context)) {
	request, writer := humago.Unwrap(ctx)
	if !acceptsEventStream(request) {
		s.writeTaskProblem(writer, errs.New(errs.KindRequestNotAcceptable, "task events require text/event-stream"))
		return
	}
	if requestHasBody(request) {
		s.writeTaskProblem(writer, errs.New(errs.KindMalformedRequest, "task event request body is not allowed"))
		return
	}
	if len(request.URL.Query()) != 0 {
		s.writeTaskProblem(writer, errs.New(errs.KindMalformedRequest, "task event query is invalid"))
		return
	}
	if ids.Validate(ids.KindTask, request.PathValue("id")) != nil {
		s.writeTaskProblem(writer, errs.New(errs.KindMalformedRequest, "task id is invalid"))
		return
	}
	if _, err := parseLastTaskEventID(request.Header); err != nil {
		s.writeTaskProblem(writer, err)
		return
	}
	next(ctx)
}

func (s *Server) taskEventStream(_ context.Context, _ *taskEventStreamInput) (*huma.StreamResponse, error) {
	return &huma.StreamResponse{Body: func(ctx huma.Context) {
		request, writer := humago.Unwrap(ctx)
		s.streamTaskEvents(writer, request)
	}}, nil
}

func (s *Server) streamTaskEvents(w http.ResponseWriter, r *http.Request) {
	if !acceptsEventStream(r) {
		s.writeTaskProblem(w, errs.New(errs.KindRequestNotAcceptable, "task events require text/event-stream"))
		return
	}
	if requestHasBody(r) {
		s.writeTaskProblem(w, errs.New(errs.KindMalformedRequest, "task event request body is not allowed"))
		return
	}
	if len(r.URL.Query()) != 0 {
		s.writeTaskProblem(w, errs.New(errs.KindMalformedRequest, "task event query is invalid"))
		return
	}
	taskID := r.PathValue("id")
	if ids.Validate(ids.KindTask, taskID) != nil {
		s.writeTaskProblem(w, errs.New(errs.KindMalformedRequest, "task id is invalid"))
		return
	}
	after, err := parseLastTaskEventID(r.Header)
	if err != nil {
		s.writeTaskProblem(w, err)
		return
	}
	if s.taskEventStreams == nil {
		s.writeTaskProblem(w, errs.New(errs.KindInternal, "task event repository is not configured"))
		return
	}

	streamContext, cancel := context.WithCancel(r.Context())
	defer cancel()
	stream, err := s.taskEventStreams.OpenTaskEventStream(streamContext, taskID, after)
	if err != nil {
		s.writeTaskProblem(w, normalizeTaskRouteError(err))
		return
	}

	frames := make(chan []byte)
	done := make(chan error, 1)
	go func() {
		streamErr := stream.Run(streamContext, func(record taskjournal.TaskEventRecord) error {
			frame, frameErr := taskEventSSEFrame(record)
			if frameErr != nil {
				return frameErr
			}
			select {
			case frames <- frame:
				return nil
			case <-streamContext.Done():
				return streamContext.Err()
			}
		})
		close(frames)
		done <- streamErr
	}()

	s.writeSSE(w, r.WithContext(streamContext), frames)
	cancel()
	streamErr := <-done
	if streamErr != nil && r.Context().Err() == nil && !errors.Is(streamErr, context.Canceled) {
		s.logTaskEventStreamFailure(taskID, streamErr)
	}
}

func parseLastTaskEventID(header http.Header) (uint64, error) {
	values := header.Values("Last-Event-ID")
	if len(values) == 0 {
		return 0, nil
	}
	if len(values) != 1 {
		return 0, errs.New(errs.KindMalformedRequest, "Last-Event-ID must appear at most once")
	}
	value := values[0]
	if value == "0" {
		return 0, nil
	}
	if len(value) == 0 || len(value) > 20 || value[0] == '0' {
		return 0, errs.New(errs.KindMalformedRequest, "Last-Event-ID is invalid")
	}
	for _, character := range []byte(value) {
		if character < '0' || character > '9' {
			return 0, errs.New(errs.KindMalformedRequest, "Last-Event-ID is invalid")
		}
	}
	sequence, err := strconv.ParseUint(value, 10, 64)
	if err != nil || strconv.FormatUint(sequence, 10) != value {
		return 0, errs.New(errs.KindMalformedRequest, "Last-Event-ID is invalid")
	}
	return sequence, nil
}

func taskEventSSEFrame(record taskjournal.TaskEventRecord) ([]byte, error) {
	state, err := taskEventAPIStatus(record.State)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(apiTypes.TaskEvent{
		Sequence: record.Sequence, StepID: record.Identity.StepID, State: state,
		Attempt: record.Identity.Attempt, Ordinal: record.Identity.Ordinal, ReceivedAt: record.ReceivedAt,
	})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	frame := make([]byte, 0, len(data)+32)
	frame = append(frame, "id: "...)
	frame = strconv.AppendUint(frame, record.Sequence, 10)
	frame = append(frame, '\n')
	frame = append(frame, "data: "...)
	frame = append(frame, data...)
	frame = append(frame, '\n', '\n')
	return frame, nil
}

func (s *Server) logTaskEventStreamFailure(taskID string, err error) {
	if s.Logger == nil {
		return
	}
	kind := "transport"
	switch {
	case errors.Is(err, errs.New(errs.KindStorageUnavailable, "")):
		kind = "storage_unavailable"
	case errors.Is(err, errs.New(errs.KindTaskNotFound, "")):
		kind = "task_removed"
	case errors.Is(err, errs.New(errs.KindInternal, "")):
		kind = "corruption"
	}
	s.Logger.Warn(
		"controller: Task event stream disconnected",
		slog.String("task_id", taskID),
		slog.String("kind", kind),
	)
}

func requestHasBody(r *http.Request) bool {
	return r.ContentLength > 0 || len(r.TransferEncoding) != 0 ||
		(r.Body != nil && r.Body != http.NoBody)
}

func taskEventOpenAPISchema() *huma.Schema {
	return &huma.Schema{
		Type: huma.TypeObject,
		Properties: map[string]*huma.Schema{
			"sequence":    {Type: huma.TypeInteger, Format: "int64"},
			"step_id":     {Type: huma.TypeString},
			"state":       {Type: huma.TypeString, Enum: taskEventStateOpenAPIValues()},
			"attempt":     {Type: huma.TypeInteger, Format: "int32"},
			"ordinal":     {Type: huma.TypeInteger, Format: "int64"},
			"received_at": {Type: huma.TypeString, Format: "date-time"},
		},
		Required:             []string{"sequence", "step_id", "state", "attempt", "ordinal", "received_at"},
		AdditionalProperties: false,
	}
}

func taskEventStreamOpenAPISchema() *huma.Schema {
	return &huma.Schema{
		Title:       "Task event stream",
		Description: "Each item describes one SSE message serialized as UTF-8 text.",
		Type:        huma.TypeArray,
		Items: &huma.Schema{
			Type: huma.TypeObject,
			Properties: map[string]*huma.Schema{
				"id":   {Type: huma.TypeString, Pattern: positiveEventIDPattern},
				"data": {Ref: "#/components/schemas/TaskEvent"},
			},
			Required:             []string{"id", "data"},
			AdditionalProperties: false,
		},
	}
}

func taskEventStateOpenAPIValues() []any {
	return []any{
		string(apiTypes.TaskPending),
		string(apiTypes.TaskRunning),
		string(apiTypes.TaskCompleted),
		string(apiTypes.TaskFailed),
		string(apiTypes.TaskAborted),
		string(apiTypes.TaskTimedOut),
	}
}
