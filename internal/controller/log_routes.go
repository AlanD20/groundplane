package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/danielgtaylor/huma/v2"
)

const maximumPublicLogLine = 32 * 1024

func (s *Server) registerLogOpenAPI() {
	s.API.OpenAPI().Components.Schemas.Map()["LogEvent"] = logEventOpenAPISchema()
	for _, operation := range []struct {
		id      string
		path    string
		tag     string
		pattern string
	}{
		{id: "environment.logs", path: "/environments/{id}/logs", tag: "Environments", pattern: "^env_[0-9A-HJKMNP-TV-Z]{26}$"},
		{id: "service.logs", path: "/services/{id}/logs", tag: "Services", pattern: "^svc_[0-9A-HJKMNP-TV-Z]{26}$"},
	} {
		s.API.OpenAPI().AddOperation(&huma.Operation{
			OperationID: operation.id,
			Method:      http.MethodGet,
			Path:        operation.path,
			Tags:        []string{operation.tag},
			Parameters: []*huma.Param{
				{Name: "id", In: "path", Required: true, Schema: &huma.Schema{Type: huma.TypeString, Pattern: operation.pattern}},
				{Name: "tail", In: "query", Schema: &huma.Schema{Type: huma.TypeInteger, Format: "int32", Minimum: logFloat(0), Maximum: logFloat(1000), Default: 200}},
				{Name: "follow", In: "query", Schema: &huma.Schema{Type: huma.TypeBoolean, Default: false}},
			},
			Responses: map[string]*huma.Response{
				"200": {
					Description: "Transient non-resumable log stream",
					Content: map[string]*huma.MediaType{
						"text/event-stream": {Schema: logEventStreamOpenAPISchema()},
					},
				},
				"default": {
					Description: "Error",
					Content: map[string]*huma.MediaType{
						"application/problem+json": {Schema: &huma.Schema{Ref: "#/components/schemas/Error"}},
					},
				},
			},
		})
	}
}

func (s *Server) environmentLogs(w http.ResponseWriter, request *http.Request) {
	s.serveLogs(w, request, ids.KindEnvironment)
}

func (s *Server) serviceLogs(w http.ResponseWriter, request *http.Request) {
	s.serveLogs(w, request, ids.KindService)
}

func (s *Server) serveLogs(w http.ResponseWriter, request *http.Request, kind ids.Kind) {
	if s.logs == nil {
		s.writeTaskProblem(w, errs.New(errs.KindInternal, "logs are not configured"))
		return
	}
	if !acceptsEventStream(request) {
		s.writeTaskProblem(w, errs.New(errs.KindRequestNotAcceptable, "logs require text/event-stream"))
		return
	}
	if len(request.Header.Values("Last-Event-ID")) != 0 {
		s.writeTaskProblem(w, errs.New(errs.KindMalformedRequest, "Last-Event-ID is not supported for logs"))
		return
	}
	if requestHasBody(request) {
		s.writeTaskProblem(w, errs.New(errs.KindMalformedRequest, "log requests must not include a body"))
		return
	}
	tail, follow, err := parseLogQuery(request)
	if err != nil {
		s.writeTaskProblem(w, err)
		return
	}
	id := request.PathValue("id")
	if ids.Validate(kind, id) != nil {
		s.writeTaskProblem(w, errs.New(errs.KindMalformedRequest, "log target id is invalid"))
		return
	}

	var subscription *agentchannel.LogSubscription
	if kind == ids.KindEnvironment {
		subscription, err = s.logs.OpenEnvironment(request.Context(), id, tail, follow)
	} else {
		subscription, err = s.logs.OpenService(request.Context(), id, tail, follow)
	}
	if err != nil {
		s.writeTaskProblem(w, normalizeProjectError(err))
		return
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(request.Context()), time.Second)
		defer cancel()
		subscription.Cancel(cleanup)
	}()

	frames := make(chan []byte)
	go encodeLogFrames(request.Context(), subscription.Events(), frames)
	s.writeSSE(w, request, frames)
}

func parseLogQuery(request *http.Request) (uint32, bool, error) {
	if !literalLogQuery(request.URL.RawQuery, request.URL.ForceQuery) {
		return 0, false, errs.New(errs.KindMalformedRequest, "log query parameters are invalid")
	}
	query, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		return 0, false, errs.New(errs.KindMalformedRequest, "log query parameters are invalid")
	}
	for key, values := range query {
		if (key != "tail" && key != "follow") || len(values) != 1 {
			return 0, false, errs.New(errs.KindMalformedRequest, "log query parameters are invalid")
		}
	}
	tail := uint64(200)
	if values, exists := query["tail"]; exists {
		raw := values[0]
		if !canonicalLogDecimal(raw) {
			return 0, false, errs.New(errs.KindMalformedRequest, "tail must be canonical decimal")
		}
		parsed, err := strconv.ParseUint(raw, 10, 16)
		if err != nil || parsed > 1000 {
			return 0, false, errs.New(errs.KindMalformedRequest, "tail must be between 0 and 1000")
		}
		tail = parsed
	}
	follow := false
	if values, exists := query["follow"]; exists {
		raw := values[0]
		if raw == "" {
			return 0, false, errs.New(errs.KindMalformedRequest, "follow must not be empty")
		}
		if raw != "true" && raw != "false" {
			return 0, false, errs.New(errs.KindMalformedRequest, "follow must be true or false")
		}
		follow = raw == "true"
	}
	return uint32(tail), follow, nil
}

func literalLogQuery(raw string, force bool) bool {
	if raw == "" {
		return !force
	}
	if strings.ContainsAny(raw, "%;") {
		return false
	}
	seen := make(map[string]struct{}, 2)
	for _, segment := range strings.Split(raw, "&") {
		key, value, found := strings.Cut(segment, "=")
		if !found || key == "" || value == "" || strings.Contains(value, "=") ||
			(key != "tail" && key != "follow") {
			return false
		}
		if _, duplicate := seen[key]; duplicate {
			return false
		}
		seen[key] = struct{}{}
	}
	return true
}

func canonicalLogDecimal(value string) bool {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

func encodeLogFrames(ctx context.Context, events <-chan *agentpb.LogEvent, output chan<- []byte) {
	defer close(output)
	var sequence uint64
	for event := range events {
		sequence++
		public, ok := publicLogEvent(sequence, event)
		if !ok {
			return
		}
		data, err := json.Marshal(public)
		if err != nil {
			return
		}
		frame := []byte(fmt.Sprintf("event: log\ndata: %s\n\n", data))
		select {
		case output <- frame:
		case <-ctx.Done():
			return
		}
	}
}

func publicLogEvent(sequence uint64, event *agentpb.LogEvent) (api.LogEvent, bool) {
	if event == nil || event.GetTimestamp() == nil || event.GetTimestamp().CheckValid() != nil ||
		ids.Validate(ids.KindEnvironment, event.GetEnvironmentId()) != nil ||
		ids.Validate(ids.KindService, event.GetServiceId()) != nil ||
		ids.Validate(ids.KindDeployment, event.GetReleaseId()) != nil || event.GetServiceName() == "" ||
		event.GetContainerId() == "" || event.GetContainerName() == "" ||
		!utf8.ValidString(event.GetServiceName()) || !utf8.ValidString(event.GetContainerId()) ||
		!utf8.ValidString(event.GetContainerName()) || !utf8.ValidString(event.GetLine()) ||
		len(event.GetLine()) > maximumPublicLogLine {
		return api.LogEvent{}, false
	}
	slots := map[agentpb.LogSlot]string{
		agentpb.LogSlot_LOG_SLOT_BLUE:      "blue",
		agentpb.LogSlot_LOG_SLOT_GREEN:     "green",
		agentpb.LogSlot_LOG_SLOT_SINGLETON: "singleton",
	}
	streams := map[agentpb.LogStream]string{
		agentpb.LogStream_LOG_STREAM_STDOUT: "stdout",
		agentpb.LogStream_LOG_STREAM_STDERR: "stderr",
	}
	slot, slotOK := slots[event.GetSlot()]
	stream, streamOK := streams[event.GetStream()]
	if !slotOK || !streamOK {
		return api.LogEvent{}, false
	}
	return api.LogEvent{
		Sequence:  sequence,
		ServiceID: event.GetServiceId(), ServiceName: event.GetServiceName(),
		ContainerID: event.GetContainerId(), ContainerName: event.GetContainerName(),
		ReleaseID: event.GetReleaseId(), Slot: slot, Stream: stream,
		Timestamp: event.GetTimestamp().AsTime(), Line: event.GetLine(), Truncated: event.GetTruncated(),
	}, true
}

func logEventOpenAPISchema() *huma.Schema {
	return &huma.Schema{
		Type: huma.TypeObject,
		Properties: map[string]*huma.Schema{
			"sequence":       {Type: huma.TypeInteger, Minimum: logFloat(1)},
			"service_id":     {Type: huma.TypeString},
			"service_name":   {Type: huma.TypeString},
			"container_id":   {Type: huma.TypeString},
			"container_name": {Type: huma.TypeString},
			"release_id":     {Type: huma.TypeString},
			"slot":           {Type: huma.TypeString, Enum: []any{"blue", "green", "singleton"}},
			"stream":         {Type: huma.TypeString, Enum: []any{"stdout", "stderr"}},
			"timestamp":      {Type: huma.TypeString, Format: "date-time"},
			"line":           {Type: huma.TypeString, MaxLength: logInt(32 * 1024)},
			"truncated":      {Type: huma.TypeBoolean},
		},
		Required: []string{
			"sequence", "service_id", "service_name", "container_id", "container_name",
			"release_id", "slot", "stream", "timestamp", "line", "truncated",
		},
		AdditionalProperties: false,
	}
}

func logEventStreamOpenAPISchema() *huma.Schema {
	return &huma.Schema{
		Type: huma.TypeArray,
		Items: &huma.Schema{
			Type: huma.TypeObject,
			Properties: map[string]*huma.Schema{
				"event": {Type: huma.TypeString, Enum: []any{"log"}},
				"data":  {Ref: "#/components/schemas/LogEvent"},
			},
			Required:             []string{"event", "data"},
			AdditionalProperties: false,
		},
	}
}

func logFloat(value float64) *float64 { return &value }

func logInt(value int) *int { return &value }
