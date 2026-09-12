package containerlogs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxContainers = 256
	maxLineBytes  = 32 * 1024
)

type DockerClient interface {
	ContainerList(context.Context, client.ContainerListOptions) (client.ContainerListResult, error)
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
	ContainerLogs(context.Context, string, client.ContainerLogsOptions) (client.ContainerLogsResult, error)
}

type Reader struct {
	docker DockerClient
}

func New() (*Reader, error) {
	docker, err := client.New(client.FromEnv)
	if err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, fmt.Errorf("create Docker log client: %w", err))
	}
	return NewReader(docker), nil
}

func NewReader(docker DockerClient) *Reader {
	return &Reader{docker: docker}
}

func (reader *Reader) Close() error {
	closer, ok := reader.docker.(io.Closer)
	if !ok {
		return nil
	}
	return closer.Close()
}

type source struct {
	requestID     string
	target        *agentpb.LogTarget
	containerID   string
	containerName string
	slot          agentpb.LogSlot
	logs          client.ContainerLogsResult
}

type sourceSet struct {
	sources   []source
	closeOnce sync.Once
	closeErr  error
}

func (reader *Reader) Open(ctx context.Context, request *agentpb.LogSubscribe) (agentprotocol.LogSourceSet, error) {
	if ctx == nil || request == nil || request.GetRequestId() == "" || request.GetTail() > 1000 ||
		len(request.GetTargets()) > 128 {
		return nil, fmt.Errorf("invalid log subscription")
	}
	targets := make(map[string]*agentpb.LogTarget, len(request.GetTargets()))
	seenServices := make(map[string]struct{}, len(request.GetTargets()))
	for _, target := range request.GetTargets() {
		if target == nil || ids.Validate(ids.KindEnvironment, target.GetEnvironmentId()) != nil ||
			ids.Validate(ids.KindService, target.GetServiceId()) != nil ||
			ids.Validate(ids.KindDeployment, target.GetReleaseId()) != nil || target.GetServiceName() == "" ||
			!utf8.ValidString(target.GetServiceName()) {
			return nil, fmt.Errorf("invalid log target")
		}
		if _, exists := seenServices[target.GetServiceId()]; exists {
			return nil, fmt.Errorf("duplicate log target")
		}
		seenServices[target.GetServiceId()] = struct{}{}
		targets[target.GetServiceId()+"\x00"+target.GetReleaseId()] = target
	}

	containers, err := reader.docker.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, fmt.Errorf("list managed log containers: %w", err))
	}
	selected := make([]container.Summary, 0)
	for _, candidate := range containers.Items {
		labels := candidate.Labels
		if labels["com.groundplane.managed"] != "true" || labels["com.groundplane.kind"] != "service" {
			continue
		}
		target := targets[labels["com.groundplane.service-id"]+"\x00"+labels["com.groundplane.release-id"]]
		if target == nil || labels["com.groundplane.environment-id"] != target.GetEnvironmentId() {
			continue
		}
		role := labels["com.groundplane.runtime-role"]
		if role != "slot" && role != "singleton" {
			continue
		}
		selected = append(selected, candidate)
	}
	if len(selected) > maxContainers {
		return nil, fmt.Errorf("log source exceeds %d containers", maxContainers)
	}
	sort.Slice(selected, func(left, right int) bool {
		leftName := containerName(selected[left])
		rightName := containerName(selected[right])
		if leftName == rightName {
			return selected[left].ID < selected[right].ID
		}
		return leftName < rightName
	})

	set := &sourceSet{sources: make([]source, 0, len(selected))}
	for _, candidate := range selected {
		inspect, inspectErr := reader.docker.ContainerInspect(ctx, candidate.ID, client.ContainerInspectOptions{})
		if inspectErr != nil {
			set.Close()
			return nil, errs.Wrap(
				errs.KindStorageUnavailable,
				fmt.Errorf("inspect log container %s: %w", candidate.ID, inspectErr),
			)
		}
		if inspect.Container.Config == nil {
			set.Close()
			return nil, fmt.Errorf("inspect log container %s returned no configuration", candidate.ID)
		}
		labels := inspect.Container.Config.Labels
		role := labels["com.groundplane.runtime-role"]
		if inspect.Container.Config.Tty {
			set.Close()
			return nil, fmt.Errorf("managed log container %s unexpectedly enables TTY", candidate.ID)
		}
		target := targets[labels["com.groundplane.service-id"]+"\x00"+labels["com.groundplane.release-id"]]
		if target == nil || labels["com.groundplane.managed"] != "true" ||
			labels["com.groundplane.kind"] != "service" ||
			labels["com.groundplane.environment-id"] != target.GetEnvironmentId() ||
			(role != "slot" && role != "singleton") {
			set.Close()
			return nil, fmt.Errorf("log container %s ownership changed", candidate.ID)
		}
		slot, slotErr := logSlot(labels)
		if slotErr != nil {
			set.Close()
			return nil, slotErr
		}
		logs, logsErr := reader.docker.ContainerLogs(ctx, candidate.ID, client.ContainerLogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Timestamps: true,
			Follow:     request.GetFollow(),
			Tail:       strconv.FormatUint(uint64(request.GetTail()), 10),
		})
		if logsErr != nil {
			set.Close()
			return nil, errs.Wrap(
				errs.KindStorageUnavailable,
				fmt.Errorf("open log container %s: %w", candidate.ID, logsErr),
			)
		}
		set.sources = append(set.sources, source{
			requestID:     request.GetRequestId(),
			target:        target,
			containerID:   candidate.ID,
			containerName: containerName(candidate),
			slot:          slot,
			logs:          logs,
		})
	}
	return set, nil
}

func (set *sourceSet) Run(ctx context.Context, output chan<- *agentpb.LogEvent) error {
	if len(set.sources) == 0 {
		return nil
	}
	errorsOut := make(chan error, len(set.sources))
	done := make(chan struct{})
	var wait sync.WaitGroup
	for index := range set.sources {
		wait.Add(1)
		go func(item source) {
			defer wait.Done()
			stdout := &lineWriter{ctx: ctx, source: item, stream: agentpb.LogStream_LOG_STREAM_STDOUT, output: output}
			stderr := &lineWriter{ctx: ctx, source: item, stream: agentpb.LogStream_LOG_STREAM_STDERR, output: output}
			_, err := stdcopy.StdCopy(stdout, stderr, item.logs)
			if err == nil {
				if err = stdout.flush(); err == nil {
					err = stderr.flush()
				}
			}
			if err != nil && ctx.Err() == nil {
				errorsOut <- fmt.Errorf("read log container %s: %w", item.containerID, err)
			}
		}(set.sources[index])
	}
	go func() {
		wait.Wait()
		close(done)
	}()

	select {
	case <-done:
		select {
		case err := <-errorsOut:
			return err
		default:
			return nil
		}
	case err := <-errorsOut:
		set.Close()
		<-done
		return err
	case <-ctx.Done():
		set.Close()
		<-done
		return ctx.Err()
	}
}

func (set *sourceSet) Close() error {
	set.closeOnce.Do(func() {
		for index := range set.sources {
			if err := set.sources[index].logs.Close(); err != nil && set.closeErr == nil {
				set.closeErr = err
			}
		}
	})
	return set.closeErr
}

type lineWriter struct {
	ctx    context.Context
	source source
	stream agentpb.LogStream
	output chan<- *agentpb.LogEvent
	buffer bytes.Buffer
}

func (writer *lineWriter) Write(input []byte) (int, error) {
	for index, value := range input {
		if value == '\n' {
			if err := writer.emit(); err != nil {
				return index, err
			}
			continue
		}
		if writer.buffer.Len() < maxLineBytes+128 {
			writer.buffer.WriteByte(value)
		}
	}
	return len(input), nil
}

func (writer *lineWriter) flush() error {
	if writer.buffer.Len() == 0 {
		return nil
	}
	return writer.emit()
}

func (writer *lineWriter) emit() error {
	raw := bytes.TrimSuffix(writer.buffer.Bytes(), []byte{'\r'})
	writer.buffer.Reset()
	separator := bytes.IndexByte(raw, ' ')
	if separator <= 0 {
		return fmt.Errorf("docker log line has no timestamp")
	}
	timestamp, err := time.Parse(time.RFC3339Nano, string(raw[:separator]))
	if err != nil {
		return fmt.Errorf("parse Docker log timestamp: %w", err)
	}
	line := strings.ToValidUTF8(string(raw[separator+1:]), "\uFFFD")
	truncated := len(line) > maxLineBytes
	if truncated {
		line = truncateUTF8(line, maxLineBytes)
	}
	event := &agentpb.LogEvent{
		RequestId:     writer.source.requestID,
		EnvironmentId: writer.source.target.GetEnvironmentId(),
		ServiceId:     writer.source.target.GetServiceId(),
		ServiceName:   writer.source.target.GetServiceName(),
		ReleaseId:     writer.source.target.GetReleaseId(),
		ContainerId:   writer.source.containerID,
		ContainerName: writer.source.containerName,
		Slot:          writer.source.slot,
		Stream:        writer.stream,
		Timestamp:     timestamppb.New(timestamp),
		Line:          line,
		Truncated:     truncated,
	}
	select {
	case writer.output <- event:
		return nil
	case <-writer.ctx.Done():
		return writer.ctx.Err()
	}
}

func truncateUTF8(value string, limit int) string {
	end := limit
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return value[:end]
}

func containerName(summary container.Summary) string {
	if len(summary.Names) == 0 {
		return summary.ID
	}
	return strings.TrimPrefix(summary.Names[0], "/")
}

func logSlot(labels map[string]string) (agentpb.LogSlot, error) {
	if labels["com.groundplane.runtime-role"] == "singleton" {
		return agentpb.LogSlot_LOG_SLOT_SINGLETON, nil
	}
	switch labels["com.groundplane.slot"] {
	case "blue":
		return agentpb.LogSlot_LOG_SLOT_BLUE, nil
	case "green":
		return agentpb.LogSlot_LOG_SLOT_GREEN, nil
	default:
		return agentpb.LogSlot_LOG_SLOT_UNSPECIFIED, fmt.Errorf("invalid managed log slot")
	}
}

var _ agentprotocol.LogReader = (*Reader)(nil)
var _ agentprotocol.LogSourceSet = (*sourceSet)(nil)
var _ io.Writer = (*lineWriter)(nil)
