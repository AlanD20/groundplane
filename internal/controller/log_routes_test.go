package controller

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	logRouteEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	logRouteServiceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	logRouteReleaseID     = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// QA: LOG-01, UI-03; pure query parsing only, not source selection or Docker tail behavior.
// Rationale: query normalization would create multiple public spellings for
// one frozen log subscription, so only exact decimal and boolean forms are accepted.
func TestParseLogQueryAcceptsOnlyCanonicalValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		wantTail   uint32
		wantFollow bool
		valid      bool
	}{
		{name: "defaults", wantTail: 200, valid: true},
		{name: "lower bound", query: "?tail=0&follow=false", valid: true},
		{name: "upper bound", query: "?tail=1000&follow=true", wantTail: 1000, wantFollow: true, valid: true},
		{name: "reversed canonical order", query: "?follow=true&tail=7", wantTail: 7, wantFollow: true, valid: true},
		{name: "leading zero", query: "?tail=01"},
		{name: "positive sign", query: "?tail=%2B1"},
		{name: "whitespace", query: "?tail=+1"},
		{name: "above maximum", query: "?tail=1001"},
		{name: "duplicate tail", query: "?tail=1&tail=2"},
		{name: "empty follow", query: "?follow="},
		{name: "numeric follow", query: "?follow=1"},
		{name: "duplicate follow", query: "?follow=true&follow=false"},
		{name: "unknown", query: "?since=1"},
		{name: "invalid escape", query: "?tail=%ZZ"},
		{name: "escaped decimal", query: "?tail=%31"},
		{name: "escaped boolean", query: "?follow=%74rue"},
		{name: "escaped key", query: "?%74ail=1"},
		{name: "semicolon separator", query: "?tail=1;follow=true"},
		{name: "bare key", query: "?tail"},
		{name: "bare query", query: "?"},
		{name: "empty first segment", query: "?&tail=1"},
		{name: "empty middle segment", query: "?tail=1&&follow=true"},
		{name: "empty trailing segment", query: "?tail=1&"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "http://controller.test/logs"+test.query, nil)
			tail, follow, err := parseLogQuery(request)
			if test.valid {
				if err != nil || tail != test.wantTail || follow != test.wantFollow {
					t.Fatalf("parseLogQuery() = %d, %t, %v; want %d, %t",
						tail, follow, err, test.wantTail, test.wantFollow)
				}
				return
			}
			if !errors.Is(err, errs.New(errs.KindMalformedRequest, "")) {
				t.Fatalf("parseLogQuery() error = %v, want malformed request", err)
			}
		})
	}
}

// QA: LOG-01/02, UI-03; direct handler admission only, not live subscriptions or cleanup.
// Rationale: logs are non-resumable bodyless SSE; violations must remain
// ordinary RFC 7807 responses before any subscription or stream header exists.
func TestLogRouteRejectsAcceptResumeAndBodyViolationsBeforeHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       io.Reader
		accept     string
		lastID     string
		wantStatus int
		wantCode   string
	}{
		{name: "missing accept", wantStatus: http.StatusNotAcceptable, wantCode: "request.not_acceptable"},
		{
			name: "wrong accept", accept: "application/json",
			wantStatus: http.StatusNotAcceptable, wantCode: "request.not_acceptable",
		},
		{
			name: "resume header", accept: "text/event-stream", lastID: "1",
			wantStatus: http.StatusBadRequest, wantCode: "validation.failed",
		},
		{
			name:       "request body",
			accept:     "text/event-stream",
			body:       strings.NewReader("{}"),
			wantStatus: http.StatusBadRequest,
			wantCode:   "validation.failed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
			server.logs = &LogService{}
			request := httptest.NewRequest(http.MethodGet, "http://controller.test/logs", test.body)
			request.SetPathValue("id", logRouteServiceID)
			if test.accept != "" {
				request.Header.Set("Accept", test.accept)
			}
			if test.lastID != "" {
				request.Header.Set("Last-Event-ID", test.lastID)
			}
			response := httptest.NewRecorder()

			server.serveLogs(response, request, ids.KindService)

			if response.Code != test.wantStatus ||
				!strings.HasPrefix(response.Header().Get("Content-Type"), "application/problem+json") {
				t.Fatalf("response = %d %q, want %d problem+json",
					response.Code, response.Header().Get("Content-Type"), test.wantStatus)
			}
			var problem errs.Problem
			if err := json.Unmarshal(response.Body.Bytes(), &problem); err != nil {
				t.Fatalf("decode log refusal: %v", err)
			}
			if problem.Type != "about:blank" || string(problem.Code) != test.wantCode ||
				problem.Status != test.wantStatus {
				t.Fatalf("log refusal = %#v, want %s/%d", problem, test.wantCode, test.wantStatus)
			}
		})
	}
}

// QA: LOG-01/02; pure event projection only, not source ownership, transport or truncation execution.
// Rationale: Controller-Agent data is private and untrusted; only the exact
// bounded public LogEvent variants may cross into the human SSE representation.
func TestPublicLogEventAcceptsExactShapeAndRejectsInvalidAgentData(t *testing.T) {
	t.Parallel()

	timestamp := time.Date(2026, time.August, 29, 12, 34, 56, 123456789, time.UTC)
	valid := validAgentLogEvent(timestamp)
	got, ok := publicLogEvent(7, valid)
	want := api.LogEvent{
		Sequence: 7, ServiceID: logRouteServiceID, ServiceName: "api",
		ContainerID: "container-1", ContainerName: "api-1", ReleaseID: logRouteReleaseID,
		Slot: "blue", Stream: "stdout", Timestamp: timestamp, Line: "ready", Truncated: false,
	}
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("publicLogEvent() = %#v, %t; want %#v, true", got, ok, want)
	}

	exactLimit := validAgentLogEvent(timestamp)
	exactLimit.Line = strings.Repeat("x", 32*1024)
	if projected, accepted := publicLogEvent(1, exactLimit); !accepted || projected.Line != exactLimit.Line {
		t.Fatal("publicLogEvent() rejected exact 32 KiB line")
	}

	tests := []struct {
		name   string
		mutate func(*agentpb.LogEvent) *agentpb.LogEvent
	}{
		{name: "nil event", mutate: func(*agentpb.LogEvent) *agentpb.LogEvent { return nil }},
		{
			name:   "missing timestamp",
			mutate: func(event *agentpb.LogEvent) *agentpb.LogEvent { event.Timestamp = nil; return event },
		},
		{name: "invalid timestamp", mutate: func(event *agentpb.LogEvent) *agentpb.LogEvent {
			event.Timestamp = &timestamppb.Timestamp{Seconds: 253402300800}
			return event
		}},
		{
			name:   "invalid environment",
			mutate: func(event *agentpb.LogEvent) *agentpb.LogEvent { event.EnvironmentId = "environment"; return event },
		},
		{
			name:   "invalid service",
			mutate: func(event *agentpb.LogEvent) *agentpb.LogEvent { event.ServiceId = "service"; return event },
		},
		{
			name:   "invalid release",
			mutate: func(event *agentpb.LogEvent) *agentpb.LogEvent { event.ReleaseId = "release"; return event },
		},
		{
			name:   "empty service name",
			mutate: func(event *agentpb.LogEvent) *agentpb.LogEvent { event.ServiceName = ""; return event },
		},
		{
			name:   "empty container id",
			mutate: func(event *agentpb.LogEvent) *agentpb.LogEvent { event.ContainerId = ""; return event },
		},
		{
			name:   "empty container name",
			mutate: func(event *agentpb.LogEvent) *agentpb.LogEvent { event.ContainerName = ""; return event },
		},
		{
			name:   "invalid utf8",
			mutate: func(event *agentpb.LogEvent) *agentpb.LogEvent { event.Line = string([]byte{0xff}); return event },
		},
		{name: "oversized line", mutate: func(event *agentpb.LogEvent) *agentpb.LogEvent {
			event.Line = strings.Repeat("x", 32*1024+1)
			return event
		}},
		{name: "unknown slot", mutate: func(event *agentpb.LogEvent) *agentpb.LogEvent {
			event.Slot = agentpb.LogSlot_LOG_SLOT_UNSPECIFIED
			return event
		}},
		{name: "unknown stream", mutate: func(event *agentpb.LogEvent) *agentpb.LogEvent {
			event.Stream = agentpb.LogStream_LOG_STREAM_UNSPECIFIED
			return event
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if event, accepted := publicLogEvent(1, test.mutate(validAgentLogEvent(timestamp))); accepted {
				t.Fatalf("publicLogEvent() accepted %#v", event)
			}
		})
	}
}

// QA: LOG-01, TASK-06, UI-03; media-type parsing only, not HTTP negotiation or stream dispatch.
// Rationale: event-stream media parameters and comma-separated Accept values
// are valid, while lookalike and malformed media types are not.
func TestAcceptsEventStreamUsesParsedMediaTypes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		accept string
		want   bool
	}{
		{accept: "text/event-stream", want: true},
		{accept: "application/json, text/event-stream; charset=utf-8", want: true},
		{accept: "text/event-streaming", want: false},
		{accept: "text/event-stream; broken", want: false},
	} {
		request := httptest.NewRequest(http.MethodGet, "http://controller.test/logs", nil)
		request.Header.Set("Accept", test.accept)
		if got := acceptsEventStream(request); got != test.want {
			t.Errorf("acceptsEventStream(%q) = %t, want %t", test.accept, got, test.want)
		}
	}
}

func validAgentLogEvent(timestamp time.Time) *agentpb.LogEvent {
	return &agentpb.LogEvent{
		RequestId: "request-1", EnvironmentId: logRouteEnvironmentID,
		ServiceId: logRouteServiceID, ServiceName: "api", ReleaseId: logRouteReleaseID,
		ContainerId: "container-1", ContainerName: "api-1", Slot: agentpb.LogSlot_LOG_SLOT_BLUE,
		Stream: agentpb.LogStream_LOG_STREAM_STDOUT, Timestamp: timestamppb.New(timestamp), Line: "ready",
	}
}
