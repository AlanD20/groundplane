package apiclient

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const taskEventReconnectDelay = time.Second

func (c *Client) ShowTask(ctx context.Context, id string) (apiTypes.Task, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Task{}, err
	}
	path := "/api/v1/tasks/" + id
	response, err := client.TaskShowWithResponse(ctx, id)
	if err != nil {
		return apiTypes.Task{}, generatedCallError(ctx, http.MethodGet, path, err)
	}
	if err := generatedResponseError(
		http.MethodGet, path, response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Task{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.Task{}
		if err := decodeSingleJSON(http.MethodGet, path, bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.Task{}, err
		}
	}
	return taskFromGenerated(*parsed), nil
}

func taskFromGenerated(task generated.Task) apiTypes.Task {
	result := apiTypes.Task{
		ID: task.Id, OperationID: task.OperationId, Type: task.Type, Target: task.Target,
		Status: apiTypes.TaskStatus(task.Status),
	}
	if task.RetryOf != nil {
		result.RetryOf = *task.RetryOf
	}
	if task.PlanHash != nil {
		result.PlanHash = *task.PlanHash
	}
	if task.Steps != nil {
		result.Steps = make([]apiTypes.TaskStep, len(*task.Steps))
		for index, step := range *task.Steps {
			result.Steps[index] = apiTypes.TaskStep{
				Name: step.Name, Status: apiTypes.TaskStatus(step.Status),
			}
		}
	}
	return result
}

// StreamTaskEvents follows one Task until its authoritative status is
// terminal. Every reconnect resumes from the last event delivered to caller.
func (c *Client) StreamTaskEvents(
	ctx context.Context,
	id string,
	onEvent func(apiTypes.TaskEvent) error,
) error {
	if onEvent == nil {
		return errs.New(errs.KindInternal, "apiclient: Task event callback is required")
	}
	lastSequence := uint64(0)
	for {
		nextSequence, err := c.streamTaskEventsOnce(ctx, id, lastSequence, onEvent)
		lastSequence = nextSequence
		if err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return contextErr
			}
			if !errs.IsRetryable(err) {
				return err
			}
			if err := waitForTaskEventReconnect(ctx); err != nil {
				return err
			}
			continue
		}

		task, err := c.ShowTask(ctx, id)
		if err != nil {
			if !errs.IsRetryable(err) {
				return err
			}
			if err := waitForTaskEventReconnect(ctx); err != nil {
				return err
			}
			continue
		}
		if terminalPublicTaskStatus(task.Status) {
			return nil
		}
		if err := waitForTaskEventReconnect(ctx); err != nil {
			return err
		}
	}
}

func (c *Client) streamTaskEventsOnce(
	ctx context.Context,
	id string,
	lastSequence uint64,
	onEvent func(apiTypes.TaskEvent) error,
) (uint64, error) {
	var params *generated.TaskEventsParams
	if lastSequence > 0 {
		lastEventID := strconv.FormatUint(lastSequence, 10)
		params = &generated.TaskEventsParams{LastEventID: &lastEventID}
	}
	request, err := generated.NewTaskEventsRequest(c.BaseURL+"/api/v1/", id, params)
	if err != nil {
		return lastSequence, errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("apiclient: build generated Task event request: %w", err),
		)
	}
	request = request.WithContext(ctx)
	request.Header.Set("Accept", "text/event-stream")
	response, err := c.StreamingHTTP.Do(request)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return lastSequence, contextErr
		}
		return lastSequence, errs.Wrap(
			errs.KindStorageUnavailable,
			fmt.Errorf("apiclient: open Task event stream: %w", err),
		)
	}
	defer response.Body.Close()
	path := "/api/v1/tasks/" + id + "/events"
	if response.StatusCode != http.StatusOK {
		return lastSequence, responseProblem(http.MethodGet, path, response)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "text/event-stream" {
		return lastSequence, errs.Newf(
			errs.KindInternal,
			"apiclient: Task event stream returned content type %q",
			response.Header.Get("Content-Type"),
		)
	}
	return consumeTaskEventStream(ctx, path, response.Body, lastSequence, onEvent)
}

func consumeTaskEventStream(
	ctx context.Context,
	path string,
	reader io.Reader,
	lastSequence uint64,
	onEvent func(apiTypes.TaskEvent) error,
) (uint64, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Split(splitEventStreamLines)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	firstLine := true
	frameID := ""
	hasID := false
	data := make([]string, 0, 1)
	hasData := false
	for scanner.Scan() {
		line := scanner.Text()
		if !utf8.ValidString(line) {
			return lastSequence, errs.New(errs.KindInternal, "apiclient: Task event stream is not UTF-8")
		}
		if firstLine {
			line = strings.TrimPrefix(line, "\uFEFF")
			firstLine = false
		}
		if line == "" {
			if !hasID && !hasData {
				continue
			}
			if !hasID || !hasData {
				return lastSequence, errs.New(errs.KindInternal, "apiclient: Task event frame is incomplete")
			}
			sequence, event, err := decodeTaskEventFrame(path, frameID, strings.Join(data, "\n"))
			if err != nil {
				return lastSequence, err
			}
			if sequence > lastSequence {
				if sequence != lastSequence+1 {
					return lastSequence, errs.New(errs.KindInternal, "apiclient: Task event sequence has a gap")
				}
				if err := onEvent(event); err != nil {
					return lastSequence, err
				}
				lastSequence = sequence
			}
			frameID = ""
			hasID = false
			data = data[:0]
			hasData = false
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if found {
			value = strings.TrimPrefix(value, " ")
		}
		switch field {
		case "id":
			if hasID {
				return lastSequence, errs.New(errs.KindInternal, "apiclient: Task event frame repeats its id")
			}
			frameID = value
			hasID = true
		case "data":
			data = append(data, value)
			hasData = true
		case "event":
			if value != "" && value != "message" {
				return lastSequence, errs.New(errs.KindInternal, "apiclient: Task event stream has an unknown event")
			}
		}
	}
	if err := scanner.Err(); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return lastSequence, contextErr
		}
		return lastSequence, errs.Wrap(
			errs.KindStorageUnavailable,
			fmt.Errorf("apiclient: read Task event stream: %w", err),
		)
	}
	if hasID || hasData {
		return lastSequence, errs.New(errs.KindStorageUnavailable, "apiclient: Task event stream ended mid-frame")
	}
	return lastSequence, nil
}

func decodeTaskEventFrame(path string, frameID string, data string) (uint64, apiTypes.TaskEvent, error) {
	sequence, err := strconv.ParseUint(frameID, 10, 64)
	if err != nil || sequence == 0 || strconv.FormatUint(sequence, 10) != frameID {
		return 0, apiTypes.TaskEvent{}, errs.New(errs.KindInternal, "apiclient: Task event id is invalid")
	}
	var event apiTypes.TaskEvent
	if err := decodeSingleJSON(http.MethodGet, path, strings.NewReader(data), &event); err != nil {
		return 0, apiTypes.TaskEvent{}, err
	}
	if event.Sequence != sequence || sequence > maximumPublicTaskEvents || event.StepID == "" ||
		event.Attempt == 0 || event.Ordinal == 0 || event.ReceivedAt.IsZero() ||
		!validPublicTaskStatus(event.State) {
		return 0, apiTypes.TaskEvent{}, errs.New(errs.KindInternal, "apiclient: Task event data is invalid")
	}
	return sequence, event, nil
}

const maximumPublicTaskEvents = 1000

func validPublicTaskStatus(status apiTypes.TaskStatus) bool {
	switch status {
	case apiTypes.TaskPending, apiTypes.TaskRunning, apiTypes.TaskCompleted,
		apiTypes.TaskFailed, apiTypes.TaskAborted, apiTypes.TaskTimedOut:
		return true
	default:
		return false
	}
}

func terminalPublicTaskStatus(status apiTypes.TaskStatus) bool {
	switch status {
	case apiTypes.TaskCompleted, apiTypes.TaskFailed, apiTypes.TaskAborted, apiTypes.TaskTimedOut:
		return true
	default:
		return false
	}
}

func waitForTaskEventReconnect(ctx context.Context) error {
	timer := time.NewTimer(taskEventReconnectDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
