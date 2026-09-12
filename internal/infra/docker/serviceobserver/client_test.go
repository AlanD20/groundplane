package serviceobserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"google.golang.org/protobuf/proto"
)

type observationTransport func(*http.Request) (*http.Response, error)

func (transport observationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

// Rationale: the real Docker SDK must carry the bounded read options and decode
// inspect evidence into the same closed result that survives the Agent wire.
// A private transport proves that seam without connecting to a host or socket.
func TestDockerClientAndObservationWire(t *testing.T) {
	request := readRequest()
	requestBytes, err := proto.Marshal(&agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_ObserveServices{ObserveServices: request},
	})
	if err != nil {
		t.Fatal(err)
	}
	decodedRequest := &agentpb.ControllerMessage{}
	if err := proto.Unmarshal(requestBytes, decodedRequest); err != nil {
		t.Fatal(err)
	}
	fixture := &readEngine{}
	fixture.add("current", readLabels(request.Targets[0], 1), &container.State{
		Status: container.StateRunning, Running: true, Health: &container.Health{Status: container.Healthy},
	})
	listBytes, err := json.Marshal(fixture.listed)
	if err != nil {
		t.Fatal(err)
	}
	inspectBytes, err := json.Marshal(fixture.inspects["current"].Container)
	if err != nil {
		t.Fatal(err)
	}
	var calls []string
	transport := observationTransport(func(httpRequest *http.Request) (*http.Response, error) {
		if httpRequest.Method != http.MethodGet || httpRequest.Body != nil {
			t.Fatalf("unexpected non-read request: %s %s", httpRequest.Method, httpRequest.URL.Path)
		}
		deadline, present := httpRequest.Context().Deadline()
		if !present || time.Until(deadline) > serviceobservation.Timeout {
			t.Fatal("Docker request lost the observation deadline")
		}
		calls = append(calls, httpRequest.URL.Path)
		var body []byte
		switch httpRequest.URL.Path {
		case "/v1.55/containers/json":
			query := httpRequest.URL.Query()
			var filters client.Filters
			if err := json.Unmarshal([]byte(query.Get("filters")), &filters); err != nil {
				t.Fatal(err)
			}
			if query.Get("all") != "1" || query.Get("limit") != "4097" || len(filters) != 1 ||
				len(
					filters["label"],
				) != 1 || !filters["label"]["com.groundplane.environment-id="+request.Targets[0].EnvironmentId] {
				t.Fatalf("Docker inventory scope changed: %v", query)
			}
			body = listBytes
		case "/v1.55/containers/current/json":
			body = inspectBytes
		default:
			t.Fatalf("unexpected Docker request: %s", httpRequest.URL.Path)
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(bytes.NewReader(body)), Request: httpRequest,
		}, nil
	})
	engine, err := client.New(client.WithAPIVersion("1.55"), client.WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	})
	observer, err := NewWithEngine(engine)
	if err != nil {
		t.Fatal(err)
	}
	result, err := observer.Observe(context.Background(), decodedRequest.GetObserveServices())
	if err != nil {
		t.Fatal(err)
	}
	resultBytes, err := proto.Marshal(&agentpb.AgentMessage{
		Payload: &agentpb.AgentMessage_ServiceObservationResult{ServiceObservationResult: result},
	})
	if err != nil {
		t.Fatal(err)
	}
	decodedResult := &agentpb.AgentMessage{}
	if err := proto.Unmarshal(resultBytes, decodedResult); err != nil {
		t.Fatal(err)
	}
	result = decodedResult.GetServiceObservationResult()
	if err := serviceobservation.ValidateResult(request, result); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(calls, []string{"/v1.55/containers/json", "/v1.55/containers/current/json"}) ||
		serviceobservation.Summarize(result.Observations[0].GetReplicas(), 1) != serviceobservation.Healthy ||
		bytes.Contains(resultBytes, []byte("must-not-leak")) {
		t.Fatalf("wrong public result or Docker reads: %v, %v", result, calls)
	}
}
