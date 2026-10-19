package releaseruntime

import (
	"context"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/agent/scriptruntime"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type nativeReleaseExecutorFixture struct {
	t            *testing.T
	input        releaseexecution.Input
	assignment   *agentpb.ReleaseWorkAssignment
	path         string
	journal      *agentworkjournal.Journal
	executor     *Executor
	last         *releaseexecution.CheckpointReport
	completion   *agentpb.ReleaseWorkCompletionRequest
	finished     time.Time
	scenario     string
	transport    error
	starts       int
	checkpoints  int
	failures     int
	completions  int
	hostStarts   int
	present      bool
	observations int
	hostActions  []releaseexecution.RolloutAction
	files        *nativeConfigurationFileHelper
}

func newNativeReleaseExecutorFixture(t *testing.T, scenario string) *nativeReleaseExecutorFixture {
	t.Helper()
	supplied := failureHookInput(t, false)
	input := supplied.Input
	input.Attempt = supplied.Forward
	input.Attempt.StartedAt, input.Attempt.FinishedAt, input.Attempt.Result, input.Attempt.Outcome = nil, nil, nil, ""
	input.Release.Hooks = nil
	directory, err := environmentpath.Derive(environmentpath.DefaultVolumeRoot, environmentpath.Scope{
		ProjectKind: environmentpath.ProjectKindTenant, TenantID: ids.New(ids.KindTenant),
		ProjectID: ids.New(ids.KindProject), EnvironmentID: input.Release.EnvironmentId})
	if err != nil {
		t.Fatal(err)
	}
	input.Release.Candidate.AuthorizedVolumeDir = directory
	input.Artifacts, err = executionplan.OpenReleaseWorkArtifacts(input.Attempt, input.Release)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	f := &nativeReleaseExecutorFixture{t: t, input: input,
		assignment: &agentpb.ReleaseWorkAssignment{Attempt: identity, Release: proto.CloneOf(input.Release)},
		path:       filepath.Join(t.TempDir(), "journal"), scenario: scenario,
		transport: errors.New("injected lost completion acknowledgement"), files: &nativeConfigurationFileHelper{}}
	f.open()
	t.Cleanup(func() {
		if err := f.journal.Close(); err != nil {
			t.Error(err)
		}
	})
	return f
}

func (f *nativeReleaseExecutorFixture) open() {
	f.t.Helper()
	var err error
	f.journal, err = agentworkjournal.Open(f.path)
	if err != nil {
		f.t.Fatal(err)
	}
	materializer, err := materialization.New(f.files, nil)
	if err != nil {
		f.t.Fatal(err)
	}
	scripts, err := scriptruntime.New(&failureHookEngine{before: func(agentpb.ScriptExecutionState) {
		f.t.Fatal("a Release without hooks invoked a Script")
	}})
	if err != nil {
		f.t.Fatal(err)
	}
	f.executor, err = NewExecutor(environmentpath.DefaultVolumeRoot, f.journal, materializer, scripts, f, f)
	if err != nil {
		f.t.Fatal(err)
	}
}

func (f *nativeReleaseExecutorFixture) Start(
	_ context.Context,
	request *agentpb.WorkStartRequest,
) (*agentpb.WorkStartAck, error) {
	f.starts++
	if !proto.Equal(request.Attempt, f.assignment.Attempt) {
		return nil, errs.New(errs.KindStateConflict, "Start changed the assigned native identity")
	}
	if f.scenario == "Start rejected" {
		return nil, errs.New(errs.KindStateConflict, "Start was not granted")
	}
	if f.input.Attempt.StartedAt == nil {
		at := time.Now().UTC()
		f.input.Attempt.StartedAt = &at
	}
	ack := &agentpb.WorkStartAck{Attempt: proto.CloneOf(request.Attempt),
		StartedAt: timestamppb.New(*f.input.Attempt.StartedAt), Reconcile: f.starts > 1}
	if f.scenario == "stale Start acknowledgement" {
		ack.Attempt.ExecutorGeneration++
	}
	return ack, nil
}

func (f *nativeReleaseExecutorFixture) ReleaseCheckpoint(_ context.Context,
	request *agentpb.ReleaseWorkCheckpointRequest,
) (*agentpb.ReleaseWorkCheckpointAck, error) {
	f.checkpoints++
	report, err := releaseexecution.DecodeCheckpointRequest(f.input, request)
	if err != nil {
		return nil, err
	}
	if f.scenario == "checkpoint rejected" {
		return nil, errs.New(errs.KindStateConflict, "checkpoint was not granted")
	}
	ack, err := releaseexecution.EncodeCheckpointAck(f.input, report)
	if err != nil {
		return nil, err
	}
	checkpoint, err := report.Checkpoint(f.input.Attempt)
	if err != nil {
		return nil, err
	}
	report.Ack = &checkpoint
	f.last = &report
	if f.scenario == "stale checkpoint acknowledgement" {
		ack.Attempt.ExecutorGeneration++
	}
	return ack, nil
}

func (f *nativeReleaseExecutorFixture) EffectEvidence(_ context.Context, identity *agentpb.WorkAttemptIdentity,
	raw []byte, reference workexecution.Input,
) (workexecution.Input, error) {
	if f.last == nil || !proto.Equal(identity, f.assignment.Attempt) {
		return workexecution.Input{}, errs.New(errs.KindStateConflict, "effect has no captured checkpoint")
	}
	if _, err := releaseexecution.DecodeEffectEvidence(f.input, *f.last, raw, reference); err != nil {
		return workexecution.Input{}, err
	}
	return reference, nil
}

func (f *nativeReleaseExecutorFixture) CompleteRelease(_ context.Context,
	request *agentpb.ReleaseWorkCompletionRequest,
) (*agentpb.ReleaseWorkCompletionAck, error) {
	f.completions++
	if !f.present || f.hostStarts != 1 || f.observations == 0 || f.last == nil ||
		f.last.Progress.Phase != releaseexecution.Complete {
		return nil, errs.New(
			errs.KindStateConflict,
			"completion preceded the simulated running workload and final checkpoint",
		)
	}
	if f.completion == nil {
		f.completion, f.finished = proto.CloneOf(request), time.Now().UTC()
	} else if !proto.Equal(f.completion, request) {
		return nil, errs.New(errs.KindStateConflict, "completion replay changed captured proof")
	}
	if f.scenario == "completion ACK lost" && f.completions == 1 {
		return nil, f.transport
	}
	return &agentpb.ReleaseWorkCompletionAck{Attempt: proto.CloneOf(request.Attempt),
		CheckpointSequence: request.CheckpointSequence, CheckpointEvidence: proto.CloneOf(request.CheckpointEvidence),
		Outcome: string(workexecution.Succeeded), FinishedAt: timestamppb.New(f.finished)}, nil
}

func (f *nativeReleaseExecutorFixture) FailRelease(_ context.Context,
	request *agentpb.ReleaseWorkFailureRequest,
) (*agentpb.ReleaseWorkFailureAck, error) {
	f.failures++
	failure := releaseexecution.Failure{AttemptID: f.input.Attempt.ID, Input: f.input.Attempt.Input,
		Reason: releaseexecution.FailureReason(request.Reason), RecordedAt: time.Now().UTC()}
	if f.last != nil {
		failure.Checkpoint = f.last.Ack
	}
	raw, reference, err := releaseexecution.EncodeFailure(f.input.Attempt, failure)
	if err != nil {
		return nil, err
	}
	identity := f.assignment.Attempt
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		return nil, err
	}
	ack := &agentpb.ReleaseWorkFailureAck{Attempt: proto.CloneOf(identity), Failure: raw,
		Evidence: &agentpb.WorkInputReference{InputId: reference.ID, Sha256: digest},
		Outcome:  string(workexecution.Failed), FinishedAt: timestamppb.New(failure.RecordedAt)}
	if f.input.Attempt.StartedAt != nil {
		ack.StartedAt = timestamppb.New(*f.input.Attempt.StartedAt)
	}
	return ack, nil
}

func (f *nativeReleaseExecutorFixture) Configuration(_ context.Context,
	request *agentpb.ReleaseConfigurationRequest,
) (*agentpb.ReleaseConfigurationManifest, error) {
	return &agentpb.ReleaseConfigurationManifest{Attempt: proto.CloneOf(request.Attempt), StageId: request.StageId,
		Configuration: proto.CloneOf(f.input.Release.Configuration)}, nil
}

func (*nativeReleaseExecutorFixture) ConfigurationFile(
	context.Context,
	*agentpb.ReleaseConfigurationFileRequest,
) (*agentpb.ReleaseConfigurationFileDelivery, error) {
	return nil, errs.New(errs.KindStateConflict, "unexpected undeclared file request")
}

func (*nativeReleaseExecutorFixture) Hook(
	context.Context,
	*agentpb.ReleaseHookRequest,
) (*agentpb.ReleaseHookDelivery, error) {
	return nil, errs.New(errs.KindStateConflict, "unexpected undeclared hook request")
}

func (*nativeReleaseExecutorFixture) Script(workexecution.Attempt) (checkpointdelivery.ScriptExchange, error) {
	return func(context.Context, *agentpb.ScriptCheckpointRequest) (*agentpb.ScriptCheckpointAck, error) {
		return nil, errs.New(errs.KindStateConflict, "unexpected Script checkpoint")
	}, nil
}

func (f *nativeReleaseExecutorFixture) ExecuteRelease(ctx context.Context,
	request *agentpb.ReleaseHelperRequest,
) (*agentpb.ReleaseHelperResponse, error) {
	input, report, err := composehelper.OpenReleaseRequest(request)
	if err != nil {
		return nil, err
	}
	saved, err := f.journal.Release(ctx, input)
	if err != nil || saved == nil || saved.Ack == nil ||
		!releaseexecution.EqualProgress(saved.Progress, report.Progress) {
		return nil, errs.New(errs.KindStateConflict, "host effect preceded its durable acknowledged checkpoint")
	}
	action := report.Progress.Action
	f.hostActions = append(f.hostActions, action)
	response := &agentpb.ReleaseHelperResponse{Succeeded: true, Inspection: request.InspectOnly}
	switch action {
	case releaseexecution.StartCandidate:
		if request.InspectOnly {
			if !f.present {
				return nil, errs.New(errs.KindStateConflict, "candidate is absent")
			}
		} else {
			if f.present {
				return nil, errs.New(errs.KindStateConflict, "a candidate start would be duplicated")
			}
			f.present, f.hostStarts, response.MutationAttempted = true, f.hostStarts+1, true
			if f.scenario == "startup result lost" {
				return nil, errs.New(errs.KindRequestFailed, "candidate was created but its result was lost")
			}
		}
	case releaseexecution.SwitchTraffic, releaseexecution.VerifyServing:
		if !f.present {
			return nil, errs.New(errs.KindStateConflict, "candidate is not running")
		}
	default:
		return nil, errs.New(errs.KindStateConflict, "unexpected host effect")
	}
	return response, nil
}

func (f *nativeReleaseExecutorFixture) ObserveRelease(
	_ context.Context,
	input releaseexecution.Input,
) (*agentpb.ObservedProject, error) {
	f.observations++
	project := &agentpb.ObservedProject{
		ProjectName: input.Artifacts.Candidate.ProjectName,
		ObservedAt:  timestamppb.Now(),
	}
	if f.present {
		service := f.input.Artifacts.Workloads[0]
		project.Containers = []*agentpb.ObservedContainer{{ContainerId: strings.Repeat("c", 64), Name: "app",
			ServiceId: service.ServiceId, ImageReference: service.ImageReference, ImageId: service.ImageReference,
			Labels: proto.CloneOf(
				service,
			).ExpectedLabels, State: agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING}}
	}
	return project, nil
}
