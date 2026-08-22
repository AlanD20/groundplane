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

// Rationale: Agent reads are operator-facing capabilities, so all three read
// operations must be generated from the same OpenAPI identities used by the CLI and Console.
func TestAgentReadRoutesAreInOpenAPI(t *testing.T) {
	t.Parallel()

	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	document, err := json.Marshal(server.API.OpenAPI())
	if err != nil {
		t.Fatalf("marshal OpenAPI: %v", err)
	}
	for _, operationID := range []string{"agent.list", "agent.show", "agent.config.show"} {
		if !strings.Contains(string(document), `"operationId":"`+operationID+`"`) {
			t.Fatalf("OpenAPI does not contain %s: %s", operationID, document)
		}
	}
}

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

	reader := &fakeAgentReader{}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Agents: reader})
	body := []byte(`{"pull_interval_seconds":5,"max_concurrent_tasks":2,"labels":{"zone":"edge"}}`)
	request := httptest.NewRequest(
		http.MethodPut,
		"/api/v1/agents/"+testAgentReadID+"/config",
		bytes.NewReader(body),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if reader.updated.PullIntervalSeconds != 5 || reader.updated.MaxConcurrentTasks != 2 ||
		reader.updated.Labels["zone"] != "edge" {
		t.Fatalf("updated config = %#v", reader.updated)
	}
	var returned apiTypes.AgentConfig
	if err := json.Unmarshal(response.Body.Bytes(), &returned); err != nil || returned.Labels["zone"] != "edge" {
		t.Fatalf("response config = %#v, %v", returned, err)
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
			if mutator.key != "" || mutator.removedAgentID != "" {
				t.Fatalf("mutation dispatched with input = %q/%q", mutator.removedAgentID, mutator.key)
			}
		})
	}
}

type fakeAgentReader struct {
	agent   apiTypes.Agent
	updated apiTypes.AgentConfig
}

type fakeAgentMutator struct {
	response       etcd.IdempotencyResponse
	key            string
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
) (apiTypes.AgentConfig, error) {
	reader.updated = config
	return config, nil
}
