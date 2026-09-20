package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const testAgentReadID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"

func TestAgentReadRoutesExposeTheConfiguredProjection(t *testing.T) {
	t.Parallel()

	reader := &fakeAgentReader{agent: apiTypes.Agent{
		ID: testAgentReadID, Host: "qa-workload-groundplane", Status: apiTypes.AgentHealthy,
		Labels: map[string]string{"arch": "arm64"}, InFlight: 2,
	}}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Agents: reader})

	tests := []struct {
		path  string
		check func(*testing.T, *httptest.ResponseRecorder)
	}{
		{path: "/api/v1/agents", check: func(t *testing.T, response *httptest.ResponseRecorder) {
			var page apiTypes.Page[apiTypes.Agent]
			if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
				t.Fatalf("decode Agent page: %v", err)
			}
			if len(page.Items) != 1 || page.Items[0].ID != testAgentReadID || page.Items[0].InFlight != 2 {
				t.Fatalf("Agent page = %#v", page)
			}
		}},
		{path: "/api/v1/agents/" + testAgentReadID, check: func(t *testing.T, response *httptest.ResponseRecorder) {
			var agent apiTypes.Agent
			if err := json.Unmarshal(response.Body.Bytes(), &agent); err != nil || agent.ID != testAgentReadID {
				t.Fatalf("Agent response = %#v, %v", agent, err)
			}
		}},
		{
			path: "/api/v1/agents/" + testAgentReadID + "/config",
			check: func(t *testing.T, response *httptest.ResponseRecorder) {
				var config apiTypes.AgentConfig
				if err := json.Unmarshal(response.Body.Bytes(), &config); err != nil || config.MaxConcurrentTasks != 3 {
					t.Fatalf("Agent config response = %#v, %v", config, err)
				}
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.path, func(t *testing.T) {
			t.Parallel()
			request := httptest.NewRequest(http.MethodGet, test.path, nil)
			response := httptest.NewRecorder()
			server.Mux.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			test.check(t, response)
		})
	}
}

func TestAgentConfigUpdateReplacesTheCompleteDocument(t *testing.T) {
	t.Parallel()

	wantBody := []byte(`{"labels":{"zone":"edge"},"max_concurrent_tasks":2,"pull_interval_seconds":5}`)
	reader := &fakeAgentReader{response: etcd.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: wantBody,
	}}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Agents: reader})
	body := []byte(`{"pull_interval_seconds":5,"max_concurrent_tasks":2,"labels":{"zone":"edge"}}`)
	request := httptest.NewRequest(
		http.MethodPut,
		"/api/v1/agents/"+testAgentReadID+"/config",
		bytes.NewReader(body),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(idempotencyKeyHeader, "agent-config-key-0001")
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if reader.updated.PullIntervalSeconds != 5 || reader.updated.MaxConcurrentTasks != 2 ||
		reader.updated.Labels["zone"] != "edge" {
		t.Fatalf("updated config = %#v", reader.updated)
	}
	if reader.key != "agent-config-key-0001" || !bytes.Equal(response.Body.Bytes(), wantBody) {
		t.Fatalf("response/key = %q/%q", response.Body.Bytes(), reader.key)
	}
}

func TestAgentEnrollReturnsExactTaskResponse(t *testing.T) {
	t.Parallel()

	want := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}
	mutator := &fakeAgentMutator{response: want}
	server := New(
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		Options{AgentMutations: mutator},
	)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/agents", nil)
	request.Header.Set(idempotencyKeyHeader, "agent-enroll-key-0001")
	response := httptest.NewRecorder()
	server.requestHandler().ServeHTTP(response, request)
	if response.Code != want.Status || response.Header().Get("Content-Type") != want.ContentKind ||
		!bytes.Equal(response.Body.Bytes(), want.Body) {
		t.Fatalf("response = %d/%q/%q", response.Code, response.Header().Get("Content-Type"), response.Body.Bytes())
	}
	if mutator.key != "agent-enroll-key-0001" {
		t.Fatalf("EnrollAgent() key = %q", mutator.key)
	}
}

func TestAgentRemoveReturnsExactTaskResponse(t *testing.T) {
	t.Parallel()

	want := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}
	mutator := &fakeAgentMutator{response: want}
	server := New(
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		Options{AgentMutations: mutator},
	)
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/agents/"+testAgentReadID, nil)
	request.Header.Set(idempotencyKeyHeader, "agent-remove-key-0001")
	response := httptest.NewRecorder()
	server.requestHandler().ServeHTTP(response, request)
	if response.Code != want.Status || response.Header().Get("Content-Type") != want.ContentKind ||
		!bytes.Equal(response.Body.Bytes(), want.Body) {
		t.Fatalf("response = %d/%q/%q", response.Code, response.Header().Get("Content-Type"), response.Body.Bytes())
	}
	if mutator.removedAgentID != testAgentReadID || mutator.key != "agent-remove-key-0001" {
		t.Fatalf("RemoveAgent() input = %q/%q", mutator.removedAgentID, mutator.key)
	}
}

func TestAgentUpdateReturnsExactTaskResponse(t *testing.T) {
	t.Parallel()

	want := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}
	mutator := &fakeAgentMutator{response: want}
	server := New(
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		Options{AgentMutations: mutator},
	)
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/agents/"+testAgentReadID+"/update",
		strings.NewReader(
			`{"image":"registry.example/agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`,
		),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(idempotencyKeyHeader, "agent-update-key-0001")
	response := httptest.NewRecorder()
	server.requestHandler().ServeHTTP(response, request)
	if response.Code != want.Status || response.Header().Get("Content-Type") != want.ContentKind ||
		!bytes.Equal(response.Body.Bytes(), want.Body) {
		t.Fatalf("response = %d/%q/%q", response.Code, response.Header().Get("Content-Type"), response.Body.Bytes())
	}
	if mutator.updatedAgentID != testAgentReadID || mutator.key != "agent-update-key-0001" {
		t.Fatalf("UpdateAgent() input = %q/%q", mutator.updatedAgentID, mutator.key)
	}
}

func TestAgentMutationsRejectBodiesBeforeDispatch(t *testing.T) {
	t.Parallel()

	for name, test := range map[string][2]string{
		"enroll": {http.MethodPost, "/api/v1/agents"},
		"remove": {http.MethodDelete, "/api/v1/agents/" + testAgentReadID},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			mutator := &fakeAgentMutator{}
			server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{AgentMutations: mutator})
			request := httptest.NewRequest(test[0], test[1], bytes.NewReader([]byte(`{}`)))
			request.Header.Set(idempotencyKeyHeader, "agent-body-key-0001")
			response := httptest.NewRecorder()
			server.requestHandler().ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if mutator.key != "" || mutator.updatedAgentID != "" || mutator.removedAgentID != "" {
				t.Fatalf(
					"mutation dispatched with input = %q/%q/%q",
					mutator.updatedAgentID,
					mutator.removedAgentID,
					mutator.key,
				)
			}
		})
	}
}

type fakeAgentReader struct {
	agent    apiTypes.Agent
	updated  apiTypes.AgentConfig
	response etcd.IdempotencyResponse
	key      string
}

type fakeAgentMutator struct {
	response       etcd.IdempotencyResponse
	key            string
	updatedAgentID string
	removedAgentID string
}

func (mutator *fakeAgentMutator) EnrollAgent(
	_ context.Context,
	key string,
) (etcd.IdempotencyResponse, error) {
	mutator.key = key
	return mutator.response, nil
}

func (mutator *fakeAgentMutator) RemoveAgent(
	_ context.Context,
	agentID string,
	key string,
) (etcd.IdempotencyResponse, error) {
	mutator.removedAgentID = agentID
	mutator.key = key
	return mutator.response, nil
}

func (mutator *fakeAgentMutator) UpdateAgent(
	_ context.Context,
	agentID string,
	_ string,
	key string,
) (etcd.IdempotencyResponse, error) {
	mutator.updatedAgentID = agentID
	mutator.key = key
	return mutator.response, nil
}

func (reader *fakeAgentReader) ListAgents(context.Context) ([]apiTypes.Agent, error) {
	return []apiTypes.Agent{reader.agent}, nil
}

func (reader *fakeAgentReader) GetAgent(context.Context, string) (apiTypes.Agent, error) {
	return reader.agent, nil
}

func (reader *fakeAgentReader) GetAgentConfig(context.Context, string) (apiTypes.AgentConfig, error) {
	return apiTypes.AgentConfig{PullIntervalSeconds: 2, MaxConcurrentTasks: 3, Labels: map[string]string{}}, nil
}

func (reader *fakeAgentReader) UpdateAgentConfig(
	_ context.Context,
	_ string,
	config apiTypes.AgentConfig,
	key string,
) (etcd.IdempotencyResponse, error) {
	reader.updated = config
	reader.key = key
	return reader.response, nil
}
