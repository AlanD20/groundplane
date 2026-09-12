package containerlogs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"google.golang.org/protobuf/proto"

	"github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	testEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testServiceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testReleaseID     = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: malformed or ambiguous Controller authority must be rejected
// before the Agent consults Docker and accidentally broadens source ownership.
func TestOpenRejectsInvalidAndDuplicateTargetsBeforeDocker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		request func() *agentpb.LogSubscribe
	}{
		{name: "nil request", request: func() *agentpb.LogSubscribe { return nil }},
		{name: "empty request id", request: func() *agentpb.LogSubscribe {
			request := validSubscribe()
			request.RequestId = ""
			return request
		}},
		{name: "tail above contract", request: func() *agentpb.LogSubscribe {
			request := validSubscribe()
			request.Tail = 1001
			return request
		}},
		{name: "invalid environment id", request: func() *agentpb.LogSubscribe {
			request := validSubscribe()
			request.Targets[0].EnvironmentId = "environment"
			return request
		}},
		{name: "invalid service id", request: func() *agentpb.LogSubscribe {
			request := validSubscribe()
			request.Targets[0].ServiceId = "service"
			return request
		}},
		{name: "invalid release id", request: func() *agentpb.LogSubscribe {
			request := validSubscribe()
			request.Targets[0].ReleaseId = "release"
			return request
		}},
		{name: "empty service name", request: func() *agentpb.LogSubscribe {
			request := validSubscribe()
			request.Targets[0].ServiceName = ""
			return request
		}},
		{name: "invalid utf8 service name", request: func() *agentpb.LogSubscribe {
			request := validSubscribe()
			request.Targets[0].ServiceName = string([]byte{0xff})
			return request
		}},
		{name: "duplicate service target", request: func() *agentpb.LogSubscribe {
			request := validSubscribe()
			duplicate := proto.CloneOf(request.Targets[0])
			duplicate.ReleaseId = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
			request.Targets = append(request.Targets, duplicate)
			return request
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			docker := &fakeDocker{}
			if _, err := NewReader(docker).Open(context.Background(), test.request()); err == nil {
				t.Fatal("Open() error = nil, want invalid subscription")
			}
			if docker.listCalls != 0 {
				t.Fatalf("ContainerList() calls = %d, want 0", docker.listCalls)
			}
		})
	}

	//lint:ignore SA1012 Intentionally verify that nil context is rejected before consulting Docker.
	if _, err := NewReader(&fakeDocker{}).Open(nil, validSubscribe()); err == nil {
		t.Fatal("Open(nil context) error = nil, want invalid subscription")
	}
}

// Rationale: only containers matching all frozen ownership, serving-release,
// and workload-role labels may be opened, in stable name then id order.
func TestOpenSelectsOwnedServingSourcesInDeterministicOrder(t *testing.T) {
	t.Parallel()

	matching := []container.Summary{
		{ID: "container-b", Names: []string{"/api-1"}, Labels: ownedLabels("slot", "blue")},
		{ID: "container-a", Names: []string{"/api-1"}, Labels: ownedLabels("singleton", "")},
		{ID: "container-z", Names: []string{"/api-2"}, Labels: ownedLabels("slot", "green")},
	}
	excluded := []container.Summary{
		{
			ID:     "unmanaged",
			Names:  []string{"/ignored-1"},
			Labels: changedLabel(ownedLabels("slot", "blue"), "com.groundplane.managed", "false"),
		},
		{
			ID:    "wrong-environment",
			Names: []string{"/ignored-2"},
			Labels: changedLabel(
				ownedLabels("slot", "blue"),
				"com.groundplane.environment-id",
				"env_01ARZ3NDEKTSV4RRFFQ69G5FAW",
			),
		},
		{
			ID:    "inactive-release",
			Names: []string{"/ignored-3"},
			Labels: changedLabel(
				ownedLabels("slot", "blue"),
				"com.groundplane.release-id",
				"dep_01ARZ3NDEKTSV4RRFFQ69G5FAW",
			),
		},
		{
			ID:     "stable-proxy",
			Names:  []string{"/ignored-4"},
			Labels: changedLabel(ownedLabels("slot", "blue"), "com.groundplane.runtime-role", "proxy"),
		},
		{
			ID:     "wrong-kind",
			Names:  []string{"/ignored-5"},
			Labels: changedLabel(ownedLabels("slot", "blue"), "com.groundplane.kind", "component"),
		},
	}
	docker := &fakeDocker{
		containers: append(matching, excluded...),
		inspects:   make(map[string]client.ContainerInspectResult),
		logs:       make(map[string]client.ContainerLogsResult),
	}
	for _, item := range matching {
		docker.inspects[item.ID] = client.ContainerInspectResult{Container: container.InspectResponse{
			ID: item.ID, Name: item.Names[0], Config: &container.Config{Labels: item.Labels},
		}}
		docker.logs[item.ID] = &countingLogStream{reader: bytes.NewReader(nil)}
	}

	opened, err := NewReader(docker).Open(context.Background(), validSubscribe())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := opened.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	})

	wantOrder := []string{"container-a", "container-b", "container-z"}
	if strings.Join(docker.inspectCalls, ",") != strings.Join(wantOrder, ",") {
		t.Fatalf("ContainerInspect() order = %q, want %q", docker.inspectCalls, wantOrder)
	}
	if len(docker.logCalls) != len(wantOrder) {
		t.Fatalf("ContainerLogs() calls = %d, want %d", len(docker.logCalls), len(wantOrder))
	}
	for index, call := range docker.logCalls {
		if call.id != wantOrder[index] || !call.options.ShowStdout || !call.options.ShowStderr ||
			!call.options.Timestamps || !call.options.Follow || call.options.Tail != "37" {
			t.Errorf("ContainerLogs() call %d = %#v", index, call)
		}
	}
}

// Rationale: a Docker inventory cannot expand one subscription beyond the
// accepted 256-source memory and goroutine bound.
func TestOpenRejectsMoreThanMaximumSources(t *testing.T) {
	t.Parallel()

	docker := &fakeDocker{containers: make([]container.Summary, maxContainers+1)}
	for index := range docker.containers {
		docker.containers[index] = container.Summary{
			ID:     "container-" + strconv.Itoa(index),
			Names:  []string{"/api-" + strconv.Itoa(index)},
			Labels: ownedLabels("singleton", ""),
		}
	}
	if _, err := NewReader(docker).Open(context.Background(), validSubscribe()); err == nil {
		t.Fatal("Open() error = nil, want source limit rejection")
	}
	if len(docker.inspectCalls) != 0 || len(docker.logCalls) != 0 {
		t.Fatalf("source setup calls = inspect %d logs %d, want 0", len(docker.inspectCalls), len(docker.logCalls))
	}
}

// Rationale: Docker transport failures are availability failures, while
// invalid managed state remains a source-integrity failure at the Agent boundary.
func TestOpenClassifiesDockerAvailabilityWithoutWeakeningManagedState(t *testing.T) {
	t.Parallel()

	want := errors.New("Docker unavailable")
	matching := container.Summary{ID: "container-1", Names: []string{"/api-1"}, Labels: ownedLabels("slot", "blue")}
	validInspect := client.ContainerInspectResult{Container: container.InspectResponse{
		ID: matching.ID, Name: matching.Names[0], Config: &container.Config{Labels: matching.Labels},
	}}
	tests := []struct {
		name   string
		docker *fakeDocker
	}{
		{name: "list", docker: &fakeDocker{listErr: want}},
		{name: "inspect", docker: &fakeDocker{
			containers: []container.Summary{matching}, inspectErrs: map[string]error{matching.ID: want},
		}},
		{name: "logs", docker: &fakeDocker{
			containers: []container.Summary{matching},
			inspects:   map[string]client.ContainerInspectResult{matching.ID: validInspect},
			logErrs:    map[string]error{matching.ID: want},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewReader(test.docker).Open(context.Background(), validSubscribe())
			if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) || !errors.Is(err, want) {
				t.Fatalf("Open() error = %v, want typed storage availability wrapping cause", err)
			}
		})
	}

	ttyInspect := validInspect
	ttyInspect.Container.Config = &container.Config{Labels: matching.Labels, Tty: true}
	_, err := NewReader(&fakeDocker{
		containers: []container.Summary{matching},
		inspects:   map[string]client.ContainerInspectResult{matching.ID: ttyInspect},
	}).Open(context.Background(), validSubscribe())
	if err == nil || errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
		t.Fatalf("TTY ownership error = %v, want non-availability source failure", err)
	}
}

// Rationale: Docker multiplexing must preserve stream identity while parsing
// timestamps, removing line endings, normalizing UTF-8, and bounding line bytes.
func TestSourceSetRunNormalizesBoundedTimestampedStreams(t *testing.T) {
	t.Parallel()

	const timestampText = "2026-08-29T12:34:56.123456789Z"
	var framed bytes.Buffer
	for _, write := range []struct {
		stream stdcopy.StdType
		line   []byte
	}{
		{stream: stdcopy.Stdout, line: []byte(timestampText + " first\r\n")},
		{stream: stdcopy.Stderr, line: append([]byte(timestampText+" invalid-"), 0xff, '\n')},
		{stream: stdcopy.Stdout, line: []byte(timestampText + " " + strings.Repeat("x", maxLineBytes+1) + "\n")},
	} {
		if err := writeDockerFrame(&framed, write.stream, write.line); err != nil {
			t.Fatalf("frame Docker log: %v", err)
		}
	}

	set := &sourceSet{sources: []source{{
		requestID: "request-1", target: validSubscribe().Targets[0],
		containerID: "container-1", containerName: "api-1", slot: agentpb.LogSlot_LOG_SLOT_BLUE,
		logs: &countingLogStream{reader: bytes.NewReader(framed.Bytes())},
	}}}
	output := make(chan *agentpb.LogEvent, 3)
	if err := set.Run(context.Background(), output); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	events := []*agentpb.LogEvent{<-output, <-output, <-output}
	wantTime, err := time.Parse(time.RFC3339Nano, timestampText)
	if err != nil {
		t.Fatalf("parse fixture timestamp: %v", err)
	}
	if events[0].GetStream() != agentpb.LogStream_LOG_STREAM_STDOUT || events[0].GetLine() != "first" ||
		events[0].GetTruncated() {
		t.Fatalf("stdout event = %#v", events[0])
	}
	if events[1].GetStream() != agentpb.LogStream_LOG_STREAM_STDERR || events[1].GetLine() != "invalid-\uFFFD" ||
		events[1].GetTruncated() {
		t.Fatalf("stderr event = %#v", events[1])
	}
	if events[2].GetStream() != agentpb.LogStream_LOG_STREAM_STDOUT || len(events[2].GetLine()) != maxLineBytes ||
		!events[2].GetTruncated() {
		t.Fatalf("truncated event line bytes = %d truncated=%t stream=%s",
			len(events[2].GetLine()), events[2].GetTruncated(), events[2].GetStream())
	}
	for _, event := range events {
		if !event.GetTimestamp().AsTime().Equal(wantTime) || event.GetRequestId() != "request-1" ||
			event.GetServiceId() != testServiceID || event.GetReleaseId() != testReleaseID {
			t.Errorf("event metadata = %#v", event)
		}
	}
}

// Rationale: cancellation and competing stream exits may all close the same
// source set; each Docker stream must be closed once and every caller sees the same result.
func TestSourceSetCloseIsConcurrentAndIdempotent(t *testing.T) {
	t.Parallel()

	closeFailure := errors.New("close failed")
	first := &countingLogStream{reader: bytes.NewReader(nil), closeErr: closeFailure}
	second := &countingLogStream{reader: bytes.NewReader(nil)}
	set := &sourceSet{sources: []source{{logs: first}, {logs: second}}}

	results := make(chan error, 32)
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- set.Close()
		}()
	}
	wait.Wait()
	close(results)
	for err := range results {
		if !errors.Is(err, closeFailure) {
			t.Errorf("Close() error = %v, want %v", err, closeFailure)
		}
	}
	if first.closes.Load() != 1 || second.closes.Load() != 1 {
		t.Fatalf("stream close counts = %d, %d, want 1, 1", first.closes.Load(), second.closes.Load())
	}
}

type fakeDocker struct {
	containers   []container.Summary
	inspects     map[string]client.ContainerInspectResult
	logs         map[string]client.ContainerLogsResult
	listCalls    int
	inspectCalls []string
	logCalls     []dockerLogCall
	listErr      error
	inspectErrs  map[string]error
	logErrs      map[string]error
}

type dockerLogCall struct {
	id      string
	options client.ContainerLogsOptions
}

func (docker *fakeDocker) ContainerList(
	context.Context,
	client.ContainerListOptions,
) (client.ContainerListResult, error) {
	docker.listCalls++
	return client.ContainerListResult{Items: docker.containers}, docker.listErr
}

func (docker *fakeDocker) ContainerInspect(
	_ context.Context,
	id string,
	_ client.ContainerInspectOptions,
) (client.ContainerInspectResult, error) {
	docker.inspectCalls = append(docker.inspectCalls, id)
	return docker.inspects[id], docker.inspectErrs[id]
}

func (docker *fakeDocker) ContainerLogs(
	_ context.Context,
	id string,
	options client.ContainerLogsOptions,
) (client.ContainerLogsResult, error) {
	docker.logCalls = append(docker.logCalls, dockerLogCall{id: id, options: options})
	return docker.logs[id], docker.logErrs[id]
}

type countingLogStream struct {
	reader   *bytes.Reader
	closes   atomic.Int32
	closeErr error
}

func (stream *countingLogStream) Read(output []byte) (int, error) {
	return stream.reader.Read(output)
}

func (stream *countingLogStream) Close() error {
	stream.closes.Add(1)
	return stream.closeErr
}

func validSubscribe() *agentpb.LogSubscribe {
	return &agentpb.LogSubscribe{
		RequestId: "request-1", Tail: 37, Follow: true,
		Targets: []*agentpb.LogTarget{{
			EnvironmentId: testEnvironmentID,
			ServiceId:     testServiceID,
			ServiceName:   "api",
			ReleaseId:     testReleaseID,
		}},
	}
}

func ownedLabels(role string, slot string) map[string]string {
	return map[string]string{
		"com.groundplane.managed":        "true",
		"com.groundplane.kind":           "service",
		"com.groundplane.environment-id": testEnvironmentID,
		"com.groundplane.service-id":     testServiceID,
		"com.groundplane.release-id":     testReleaseID,
		"com.groundplane.runtime-role":   role,
		"com.groundplane.slot":           slot,
	}
}

func changedLabel(labels map[string]string, key string, value string) map[string]string {
	changed := make(map[string]string, len(labels))
	for label, current := range labels {
		changed[label] = current
	}
	changed[key] = value
	return changed
}

func writeDockerFrame(output io.Writer, stream stdcopy.StdType, data []byte) error {
	var header [8]byte
	header[0] = byte(stream)
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	if _, err := output.Write(header[:]); err != nil {
		return err
	}
	_, err := output.Write(data)
	return err
}
