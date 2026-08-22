package controller

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

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
		{path: "/api/v1/agents/" + testAgentReadID + "/config", check: func(t *testing.T, response *httptest.ResponseRecorder) {
			var config apiTypes.AgentConfig
			if err := json.Unmarshal(response.Body.Bytes(), &config); err != nil || config.MaxConcurrentTasks != 3 {
				t.Fatalf("Agent config response = %#v, %v", config, err)
			}
		}},
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

type fakeAgentReader struct {
	agent apiTypes.Agent
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
