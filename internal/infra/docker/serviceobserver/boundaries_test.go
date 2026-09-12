package serviceobserver

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"google.golang.org/protobuf/proto"
)

// Rationale: late per-Service failure must discard that Service's partial counts
// without replacing the request order or discarding another Service's evidence.
func TestObservationBatchKeepsOrderAndIsolatesFailure(t *testing.T) {
	request := readRequest()
	worker := proto.CloneOf(request.Targets[0])
	worker.ServiceId, worker.ComposeName = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW", "worker"
	request.Targets = append(request.Targets, worker)
	engine := &readEngine{inspectErrors: map[string]error{"late": errs.New(errs.KindInternal, "private failure")}}
	engine.add("worker", readLabels(worker, 1), &container.State{Status: container.StateRunning, Running: true})
	engine.add(
		"first",
		readLabels(request.Targets[0], 1),
		&container.State{Status: container.StateRunning, Running: true},
	)
	engine.add("late", readLabels(request.Targets[0], 2), nil)
	result := runRead(t, engine, request)
	if !result.Observations[0].GetUnavailable() || result.Observations[0].GetReplicas() != nil ||
		result.Observations[1].GetReplicas().GetRunning() != 1 {
		t.Fatalf("partial or cross-Service result: %v", result)
	}
	if !slices.Equal(engine.calls, []string{"list", "first", "late", "worker"}) {
		t.Fatalf("unexpected inspections: %v", engine.calls)
	}
}

// Rationale: corruption already visible in list must not look like an absent
// workload or allow a malformed replica to be skipped beside a healthy one.
func TestObservationRejectsMalformedSelectedInventory(t *testing.T) {
	for name, mutate := range map[string]func(map[string]string){
		"generation":       func(labels map[string]string) { labels["com.groundplane.render-generation"] = "2" },
		"plan":             func(labels map[string]string) { labels["com.groundplane.plan-id"] = "plan_old" },
		"Compose name":     func(labels map[string]string) { labels["com.docker.compose.service"] = "wrong" },
		"missing ordinal":  func(labels map[string]string) { delete(labels, "com.docker.compose.container-number") },
		"zero ordinal":     func(labels map[string]string) { labels["com.docker.compose.container-number"] = "0" },
		"padded ordinal":   func(labels map[string]string) { labels["com.docker.compose.container-number"] = "01" },
		"overflow ordinal": func(labels map[string]string) { labels["com.docker.compose.container-number"] = "4294967296" },
	} {
		t.Run(name, func(t *testing.T) {
			request := readRequest()
			labels := readLabels(request.Targets[0], 1)
			mutate(labels)
			engine := &readEngine{}
			engine.add("bad", labels, nil)
			if !runRead(t, engine, request).Observations[0].GetUnavailable() || len(engine.calls) != 1 {
				t.Fatalf("corrupt selected inventory was inspected or accepted: %v", engine.calls)
			}
		})
	}
}

// Rationale: stopped containers must be listed, an overflow sentinel must not
// become a truncated sample, and unrelated Components/one-offs need no inspect.
func TestObservationInventoryIsScopedAndComplete(t *testing.T) {
	request := readRequest()
	engine := &readEngine{}
	labels := readLabels(request.Targets[0], 1)
	component := maps.Clone(labels)
	component["com.groundplane.component-id"] = "cmp_hidden"
	engine.add("component", component, nil)
	oneoff := maps.Clone(labels)
	oneoff["com.docker.compose.oneoff"] = "True"
	engine.add("oneoff", oneoff, nil)
	if counts := runRead(t, engine, request).Observations[0].GetReplicas(); counts == nil ||
		serviceobservation.Total(counts) != 0 {
		t.Fatalf("unrelated containers counted: %v", counts)
	}
	wantLabel := "com.groundplane.environment-id=" + request.Targets[0].EnvironmentId
	if !engine.options.All || engine.options.Limit != serviceobservation.MaximumContainers+1 ||
		engine.options.Size || len(engine.options.Filters) != 1 ||
		len(
			engine.options.Filters["label"],
		) != 1 || !engine.options.Filters["label"][wantLabel] || len(engine.calls) != 1 {
		t.Fatalf("unbounded or unscoped inventory: %+v calls=%v", engine.options, engine.calls)
	}
	engine.listErr = errs.New(errs.KindStorageUnavailable, "private Docker diagnostic")
	observer, err := NewWithEngine(engine)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := observer.Observe(context.Background(), request); err == nil || result != nil {
		t.Fatalf("failed list became evidence: result=%v err=%v", result, err)
	}
}

type deadlineEngine struct {
	readEngine
	block string
}

func (engine *deadlineEngine) ContainerList(
	ctx context.Context,
	options client.ContainerListOptions,
) (client.ContainerListResult, error) {
	if engine.block == "list" {
		<-ctx.Done()
		return client.ContainerListResult{}, ctx.Err()
	}
	return engine.readEngine.ContainerList(ctx, options)
}

func (engine *deadlineEngine) ContainerInspect(
	ctx context.Context,
	id string,
	options client.ContainerInspectOptions,
) (client.ContainerInspectResult, error) {
	if engine.block == "inspect" {
		<-ctx.Done()
		return client.ContainerInspectResult{}, ctx.Err()
	}
	return engine.readEngine.ContainerInspect(ctx, id, options)
}

// Rationale: cancellation during either Docker call must end the whole read
// with no usable partial result, even when earlier work succeeded.
func TestObservationPropagatesDeadlineThroughDockerCalls(t *testing.T) {
	for _, block := range []string{"list", "inspect"} {
		t.Run(block, func(t *testing.T) {
			request := readRequest()
			engine := &deadlineEngine{block: block}
			engine.add("current", readLabels(request.Targets[0], 1), nil)
			observer, err := NewWithEngine(engine)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			result, err := observer.Observe(ctx, request)
			if !errors.Is(err, context.DeadlineExceeded) || result != nil {
				t.Fatalf("deadline became a result: result=%v err=%v", result, err)
			}
		})
	}
}
