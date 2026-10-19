package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/volumeruntime"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/volume"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// TASK-117: public acceptance, native assignment/Start, Agent executor, durable
// checkpoint journal and actual Controller settlement must agree. Effects and
// observations are simulated, not actual Docker or directory deletion. No old
// WorkerPool, plan resolver or generic removal runtime participates.
func TestNativeVolumeExecutorCreateRemoveSettlesExactResource(t *testing.T) {
	f := newNativeVolumeFixture(t)
	journal, err := agentworkjournal.Open(filepath.Join(t.TempDir(), "journal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := journal.Close(); err != nil {
			t.Error(err)
		}
	})
	owner, err := volume.NewAgentWork(f.inputs, f.queue)
	if err != nil {
		t.Fatal(err)
	}
	starts, err := workdispatch.NewAgentStarts(f.queue, []workdispatch.AgentStartRegistration{
		{Operation: workexecution.Volume, Owner: owner},
	})
	if err != nil {
		t.Fatal(err)
	}
	host := &nativeVolumeExecutorHost{fixture: f, journal: journal}
	executor, err := volumeruntime.NewExecutor("/var/lib/groundplane/vol", journal, host)
	if err != nil {
		t.Fatal(err)
	}
	exchange := &nativeVolumeExecutorExchange{fixture: f, starts: starts}
	run := func(view workqueue.ResumeView, expected []volumeexecution.Stage) {
		t.Helper()
		assignment, err := owner.LoadWorkAssignment(t.Context(), view)
		if err != nil {
			t.Fatal(err)
		}
		input, err := volumeruntime.DecodeAssignment(assignment, f.authorization.Identity())
		if err != nil {
			t.Fatal(err)
		}
		host.view, host.procedure = view, assignment.Input
		host.applied = nil
		terminal, err := executor.Run(t.Context(), input, exchange)
		if err != nil || terminal.Outcome != workexecution.Succeeded || terminal.FinishedAt == nil ||
			terminal.ID != view.Attempt.Attempt.ID || !slices.Equal(host.applied, expected) {
			t.Fatalf(
				"native execution/settlement mismatch: terminal=%+v effects=%v err=%v",
				terminal,
				host.applied,
				err,
			)
		}
		saved, err := f.queue.ReadResult(t.Context(), terminal.TaskID, terminal.WorkItemID,
			terminal.ID, terminal.Executor, false)
		if err != nil || saved.Work.Item.Item.Phase != workexecution.Settled ||
			saved.Attempt.Attempt.Result == nil || *saved.Attempt.Attempt.Result != *terminal.Result {
			t.Fatalf("Agent completion disagrees with durable resource result: %v", err)
		}
		if err := executor.Retire(t.Context(), terminal); err != nil {
			t.Fatal(err)
		}
	}
	run(
		f.assign(t, f.created.TaskID),
		[]volumeexecution.Stage{volumeexecution.PrepareDirectory, volumeexecution.PrepareVolume},
	)
	if applied, err := f.inputs.ReadApplied(t.Context(), f.created.Volume.ID, 0); err != nil || applied == nil {
		t.Fatalf("successful Agent create did not publish applied storage: %v", err)
	}
	rename, err := f.mutations.EditVolume(t.Context(), f.created.Volume.ID,
		api.VolumeEdit{Slug: "renamed"}, "native-volume-executor-rename")
	if err != nil {
		t.Fatal(err)
	}
	var edited api.VolumeMutationResponse
	if err := json.Unmarshal(rename.Body, &edited); err != nil {
		t.Fatal(err)
	}
	// Renaming must verify the existing storage, not recreate it.
	run(f.assign(t, edited.TaskID), nil)
	impact, err := f.reads.GetVolumeDeletionImpact(t.Context(), f.created.Volume.ID, "", 40)
	if err != nil || !impact.Complete {
		t.Fatalf("removal impact: %v", err)
	}
	releaseSource := nativeVolumePreparedSource(t, f)
	beforeHead, found, err := f.blueprint.GetEnvironmentBlueprintHead(t.Context(), f.environmentID)
	if err != nil || !found {
		t.Fatalf("missing native Volume desired head: %v", err)
	}
	beforeTasks, err := f.queue.ListTasks(t.Context(), f.environmentID, "", 10, 0)
	if err != nil || beforeTasks.More {
		t.Fatalf("Task inventory before protected removal: %v", err)
	}
	if _, err := f.mutations.RemoveVolume(t.Context(), f.created.Volume.ID, impact.ImpactToken,
		"data", "native-volume-source-protected-remove"); !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("prepared Script source did not protect native removal: %v", err)
	}
	afterHead, found, err := f.blueprint.GetEnvironmentBlueprintHead(t.Context(), f.environmentID)
	if err != nil || !found || afterHead.Revision != beforeHead.Revision {
		t.Fatalf("refused removal changed desired state: %v", err)
	}
	afterTasks, err := f.queue.ListTasks(t.Context(), f.environmentID, "", 10, 0)
	if err != nil || afterTasks.More || !slices.EqualFunc(beforeTasks.Tasks, afterTasks.Tasks,
		func(before, after workqueue.TaskSnapshot) bool {
			return before.Task.ID == after.Task.ID && before.Revision == after.Revision
		}) {
		t.Fatal("refused removal changed desired state or published work")
	}
	releaseSource()
	response, err := f.mutations.RemoveVolume(t.Context(), f.created.Volume.ID, impact.ImpactToken,
		"data", "native-volume-executor-remove")
	if err != nil {
		t.Fatal(err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatal(err)
	}
	run(f.assign(t, accepted.TaskID), []volumeexecution.Stage{
		volumeexecution.DetachConsumers, volumeexecution.RemoveVolume, volumeexecution.RemoveDirectory,
	})
	if applied, err := f.inputs.ReadApplied(t.Context(), f.created.Volume.ID, 0); err != nil || applied != nil {
		t.Fatalf("successful Agent removal retained applied storage: %v", err)
	}
	projection, found, err := f.blueprint.GetEnvironmentComposeProjection(t.Context(), f.environmentID)
	if err != nil || !found || len(projection.Record.Volumes) != 0 {
		t.Fatalf("successful Agent removal retained desired declaration: %v", err)
	}
}

func nativeVolumePreparedSource(t *testing.T, f *nativeVolumeFixture) func() {
	t.Helper()
	authority, err := scriptsourcepublication.NewAuthority(f.store)
	if err != nil {
		t.Fatal(err)
	}
	executionID, snapshotID := ids.NewULID(), ids.NewULID()
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&agentpb.ResolvedRunnerSnapshot{
		SnapshotId: snapshotID, ScriptExecutionId: executionID, EnvironmentId: f.environmentID,
		Mounts: []*agentpb.ScriptRunnerMount{{SourceId: f.created.Volume.ID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	raw, err := recordcodec.Encode("script-runner-snapshot", scriptsourceevidence.StoredScriptRunnerSnapshot{
		ExecutionID: executionID, SnapshotID: snapshotID, SHA256: hex.EncodeToString(digest[:]), Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(raw)
	key := scriptexecutions.ScriptRunnerSnapshotKey(snapshotID)
	revision, err := f.store.Put(t.Context(), key, raw)
	if err != nil {
		t.Fatal(err)
	}
	operationID := ids.New(ids.KindOperation)
	members := []scriptsourceevidence.ScriptSourcePreparationMember{{Reference: scriptsourcereference.Reference{
		OperationID: operationID, ScriptExecutionID: executionID,
		Source: scriptsourcereference.SourceIdentity{
			Kind:     scriptsourcereference.SourceVolume,
			VolumeID: f.created.Volume.ID,
		},
		SourceOwnerID: f.environmentID, SourceModRevision: revision, SourceDigest: hex.EncodeToString(digest[:]),
	}, Evidence: scriptsourceevidence.ScriptSourceEvidence{
		Existing: &scriptsourceevidence.ScriptExistingSourceEvidence{SourceKey: key},
	}}}
	if _, err := authority.Prepare(t.Context(), operationID, members); err != nil {
		t.Fatal(err)
	}
	return func() {
		if err := authority.Abandon(t.Context(), operationID, members); err != nil {
			t.Fatal(err)
		}
	}
}

type nativeVolumeExecutorExchange struct {
	fixture *nativeVolumeFixture
	starts  *workdispatch.AgentStarts
}

func (exchange *nativeVolumeExecutorExchange) StartAttempt(ctx context.Context,
	attempt workexecution.Attempt,
) (*agentpb.WorkStartAck, error) {
	wire, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, err
	}
	return exchange.starts.Start(ctx, exchange.fixture.authorization, &agentpb.WorkStartRequest{Attempt: wire})
}
func (exchange *nativeVolumeExecutorExchange) VolumeCheckpoint(ctx context.Context,
	request *agentpb.VolumeWorkCheckpointRequest,
) (*agentpb.VolumeWorkCheckpointAck, error) {
	return exchange.fixture.reports.CheckpointVolume(ctx, exchange.fixture.authorization, request)
}
func (exchange *nativeVolumeExecutorExchange) CompleteVolume(ctx context.Context,
	request *agentpb.VolumeWorkCompletionRequest,
) (*agentpb.VolumeWorkCompletionAck, error) {
	return exchange.fixture.reports.CompleteVolume(ctx, exchange.fixture.authorization, request)
}
func (exchange *nativeVolumeExecutorExchange) FailVolume(ctx context.Context,
	request *agentpb.VolumeWorkFailureRequest,
) (*agentpb.VolumeWorkFailureAck, error) {
	return exchange.fixture.reports.FailVolume(ctx, exchange.fixture.authorization, request)
}
func (exchange *nativeVolumeExecutorExchange) RecoverVolume(ctx context.Context,
	request *agentpb.VolumeWorkRecoveryReportRequest,
) (*agentpb.VolumeWorkRecoveryReportAck, error) {
	return exchange.fixture.reports.RecoverVolume(ctx, exchange.fixture.authorization, request)
}

type nativeVolumeExecutorHost struct {
	fixture                                        *nativeVolumeFixture
	journal                                        *agentworkjournal.Journal
	view                                           workqueue.ResumeView
	procedure                                      *agentpb.VolumeWorkInput
	applied                                        []volumeexecution.Stage
	directoryReady, volumeReady, consumersDetached bool
}

func (host *nativeVolumeExecutorHost) Apply(ctx context.Context, _ volumeruntime.Input,
	stage volumeexecution.Stage,
) error {
	attempt := host.view.Attempt.Attempt
	current, err := host.fixture.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	saved, err := host.journal.Volume(ctx, current.Attempt.Attempt, host.procedure.Action, host.procedure.VolumeId)
	if err != nil {
		return err
	}
	if saved == nil || !saved.Acknowledged || !saved.Progress.InFlight || saved.Progress.Stage != stage ||
		current.Checkpoint.Record == nil || current.Checkpoint.Record.Sequence != saved.Progress.Sequence ||
		slices.Contains(host.applied, stage) {
		return errs.New(errs.KindStateConflict, "effect lacks its exact Controller and local journal acknowledgement")
	}
	host.applied = append(host.applied, stage)
	switch stage {
	case volumeexecution.PrepareDirectory:
		host.directoryReady = true
	case volumeexecution.PrepareVolume:
		host.volumeReady = true
	case volumeexecution.DetachConsumers:
		host.consumersDetached = true
	case volumeexecution.RemoveVolume:
		host.volumeReady = false
	case volumeexecution.RemoveDirectory:
		host.directoryReady = false
	default:
		return errs.New(errs.KindInternal, "unexpected mutation during native Volume verification")
	}
	return nil
}

func (host *nativeVolumeExecutorHost) Inspect(_ context.Context, _ volumeruntime.Input,
	_ volumeexecution.Stage,
) (volumeexecution.Observation, error) {
	return volumeexecution.Observation{VolumeID: host.procedure.VolumeId, Input: host.view.Attempt.Attempt.Input,
		At: time.Now().UTC(), DirectoryReady: host.directoryReady, VolumeReady: host.volumeReady,
		ConsumersDetached: host.consumersDetached, VolumeAbsent: !host.volumeReady,
		DirectoryAbsent: !host.directoryReady}, nil
}

func (*nativeVolumeExecutorHost) InspectRecovery(
	context.Context,
	volumeruntime.Input,
) (volumeexecution.Observation, error) {
	return volumeexecution.Observation{}, errs.New(
		errs.KindInternal,
		"unexpected cleanup in successful native Volume journey",
	)
}
