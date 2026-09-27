package runnerproxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	dockerspec "github.com/moby/docker-image-spec/specs-go/v1"
	"github.com/moby/moby/api/types/image"
	"github.com/moby/moby/api/types/system"
	"github.com/moby/moby/client"
)

type buildDaemonFixture struct {
	ledger           *BuildLedger
	images           []image.Summary
	started          int
	labels           map[string]string
	stream           string
	transportFailure bool
	ambiguous        bool
	wrongInspect     bool
	requestRecorded  bool
}

func (daemon *buildDaemonFixture) Info(context.Context, client.InfoOptions) (client.SystemInfoResult, error) {
	return client.SystemInfoResult{Info: system.Info{ID: "private-daemon"}}, nil
}

func (daemon *buildDaemonFixture) ImageList(context.Context, client.ImageListOptions) (client.ImageListResult, error) {
	return client.ImageListResult{Items: daemon.images}, nil
}

func (daemon *buildDaemonFixture) ImageBuild(
	_ context.Context,
	input io.Reader,
	options client.ImageBuildOptions,
) (client.ImageBuildResult, error) {
	daemon.started++
	issued, err := readBuildRecord[BuildIntent](daemon.ledger.root, options.Labels[buildOperationLabel]+".issued.json")
	if err != nil {
		return client.ImageBuildResult{}, err
	}
	daemon.requestRecorded = issued.RunnerID == options.Labels[runnerOwnerLabel] &&
		issued.ContextDigest == options.Labels[buildInputLabel]
	if _, err := io.Copy(io.Discard, input); err != nil {
		return client.ImageBuildResult{}, err
	}
	daemon.labels = options.Labels
	daemon.images = append(
		daemon.images,
		image.Summary{ID: "sha256:" + strings.Repeat("a", 64), Labels: options.Labels},
	)
	if daemon.ambiguous {
		daemon.images = append(
			daemon.images,
			image.Summary{ID: "sha256:" + strings.Repeat("b", 64), Labels: options.Labels},
		)
	}
	if daemon.transportFailure {
		return client.ImageBuildResult{}, io.ErrUnexpectedEOF
	}
	return client.ImageBuildResult{Body: io.NopCloser(strings.NewReader(daemon.stream))}, nil
}

func (daemon *buildDaemonFixture) ImageInspect(
	_ context.Context,
	id string,
	_ ...client.ImageInspectOption,
) (client.ImageInspectResult, error) {
	labels := daemon.labels
	if daemon.wrongInspect {
		labels = map[string]string{}
	}
	config := &dockerspec.DockerOCIImageConfig{}
	config.Labels = labels
	return client.ImageInspectResult{
		InspectResponse: image.InspectResponse{ID: id, Config: config},
	}, nil
}

// RUN-13: even success-looking output cannot authorize image delivery without
// one new image whose independently inspected identity matches this operation.
func TestBuildExecutionRequiresDurableIntentAndIndependentImageEvidence(t *testing.T) {
	for _, scenario := range []string{"success", "ambiguous-images", "changed-inspection", "transport-loss", "build-failure"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			policy, prepared := policyContext(t)
			ledger, err := OpenBuildLedger(ctx, privateSpool(t), policy.runnerID, policy.epoch)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := ledger.Close(); err != nil {
					t.Error(err)
				}
			})
			daemon := &buildDaemonFixture{
				ledger: ledger,
				images: []image.Summary{},
				stream: "{\"stream\":\"Successfully built fake-id\\n\"}\n",
			}
			daemon.ambiguous = scenario == "ambiguous-images"
			daemon.wrongInspect = scenario == "changed-inspection"
			daemon.transportFailure = scenario == "transport-loss"
			if scenario == "build-failure" {
				daemon.stream = "{\"error\":\"exit status 1\",\"errorDetail\":{\"message\":\"exit status 1\"}}\n"
			}
			executor, err := NewBuildExecutor(daemon, policy, ledger, &sync.Mutex{}, "private-daemon")
			if err != nil {
				t.Fatal(err)
			}
			operationID := ids.New(ids.KindOperation)
			var output bytes.Buffer
			result, buildErr := executor.Build(ctx, operationID, "", prepared, &output)
			if !daemon.requestRecorded || daemon.started != 1 {
				t.Fatal("daemon mutated before durable intent")
			}
			stored, readErr := ledger.Receipt(ctx, operationID)
			switch scenario {
			case "success":
				if buildErr != nil || readErr != nil || stored.ImageID != "sha256:"+strings.Repeat("a", 64) ||
					!stored.Succeeded || stored.ImageID != result.ImageID || output.String() != daemon.stream {
					t.Fatalf("successful build receipt/output: %#v, %v, %v", stored, buildErr, readErr)
				}
			case "build-failure":
				if buildErr == nil || readErr != nil || stored.Succeeded || stored.ImageID != "" ||
					len(stored.NewImages) != 1 {
					t.Fatalf(
						"failed build must retain residue without authorizing delivery: %#v, %v, %v",
						stored,
						buildErr,
						readErr,
					)
				}
			default:
				if buildErr == nil || readErr == nil {
					t.Fatal("unproved build was sealed as complete")
				}
				_, nextContext := policyContext(t)
				if _, err := executor.Build(ctx, ids.New(ids.KindOperation), "", nextContext, io.Discard); err == nil ||
					daemon.started != 1 {
					t.Fatal("uncertain effect permitted another build")
				}
			}
		})
	}
}

// RUN-13: only one process owns the ledger; restart retains an interrupted
// build's fence, and a result for another intent cannot clear that fence.
func TestBuildLedgerRetainsUnknownOutcomeAcrossReopen(t *testing.T) {
	ctx := context.Background()
	root, runnerID := privateSpool(t), ids.New(ids.KindRunner)
	ledger, err := OpenBuildLedger(ctx, root, runnerID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, err := OpenBuildLedger(ctx, root, runnerID, 1); err == nil {
		_ = duplicate.Close()
		t.Fatal("second process acquired build authority")
	}
	intent := BuildIntent{OperationID: ids.New(ids.KindOperation), RunnerID: runnerID, RuntimeEpoch: 1,
		DaemonID: "private-daemon", ContextDigest: "sha256:" + strings.Repeat("c", 64),
		OptionsDigest: "sha256:" + strings.Repeat("d", 64), BeforeImages: []string{},
	}
	if err := ledger.Issue(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenBuildLedger(ctx, root, runnerID, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	next := intent
	next.OperationID = ids.New(ids.KindOperation)
	if err := reopened.Issue(ctx, next); err == nil {
		t.Fatal("restart lost issued build fence")
	}
	wrong := BuildReceipt{Intent: next, IntentDigest: buildRecordDigest(next), NewImages: []string{}}
	if err := reopened.Complete(ctx, wrong); err == nil {
		t.Fatal("unissued result completed")
	}
	failed := BuildReceipt{Intent: intent, IntentDigest: buildRecordDigest(intent), NewImages: []string{}}
	if err := reopened.Complete(ctx, failed); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Complete(ctx, failed); err == nil {
		t.Fatal("immutable result was overwritten")
	}
	if err := reopened.Issue(ctx, next); err != nil {
		t.Fatalf("settled failure blocked next build: %v", err)
	}
}

// RUN-13: truncation or an output transport failure leaves the operation unknown
// rather than turning a partial success-looking stream into a completed build.
func TestBuildStreamRequiresCompleteFramesAndSuccessfulDelivery(t *testing.T) {
	for _, input := range []string{"", "{\"stream\":", "null\n", strings.Repeat("a", 1<<20)} {
		if _, err := forwardBuildOutput(strings.NewReader(input), io.Discard); err == nil {
			t.Fatal("invalid build stream accepted")
		}
	}
	if _, err := forwardBuildOutput(strings.NewReader("{\"stream\":\"step\"}\n"), failedBuildWriter{}); err == nil {
		t.Fatal("output loss completed build")
	}
}

type failedBuildWriter struct{}

func (failedBuildWriter) Write([]byte) (int, error) { return 0, errors.New("disconnected") }
