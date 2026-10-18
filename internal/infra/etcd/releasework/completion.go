package releasework

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type CompletionProof struct {
	Runtime    executionplan.CandidateRuntime
	Progress   Snapshot
	Serving    *agentpb.ReleaseHelperResponse
	Evidence   releaseexecution.EffectEvidence
	Conditions []keyvalue.Condition
}

// LoadCompletionProof proves that the complete typed sequence (including hooks)
// settled and retains the exact final serving observation. This is not a fresh
// host probe or a claim that a successful command alone activated the Release.
// Resource settlement and work completion must commit with these comparisons.
func LoadCompletionProof(ctx context.Context, store Reader, view workqueue.ResumeView, input releaseexecution.Input) (CompletionProof, error) {
	var empty CompletionProof
	if ctx == nil || store == nil || !view.ActiveAttempt || view.ExecutorRevision <= 0 || !workexecution.SameAssignment(input.Attempt, view.Attempt.Attempt) ||
		view.Attempt.Attempt.StartedAt == nil || input.Attempt.StartedAt == nil ||
		!view.Attempt.Attempt.StartedAt.Equal(*input.Attempt.StartedAt) ||
		view.Attempt.Attempt.FinishedAt != nil || view.Checkpoint.Record == nil {
		return empty, invalid()
	}
	saved, err := Load(ctx, store, input, view.Work.ReadRevision)
	if err != nil {
		return empty, err
	}
	if saved.Record == nil || saved.Record.Serving == nil || saved.Record.Progress.Phase != releaseexecution.Complete || saved.Record.Progress.InFlight ||
		saved.Revision != view.Checkpoint.Revision {
		return empty, invalid()
	}
	report := releaseexecution.CheckpointReport{Sequence: saved.Record.CheckpointSequence, StageID: saved.Record.StageID, Progress: saved.Record.Progress}
	checkpoint, err := report.Checkpoint(input.Attempt)
	if err != nil || !releaseexecution.EqualCheckpoint(checkpoint, *view.Checkpoint.Record) {
		return empty, invalid()
	}
	serving := saved.Record.Serving
	key := EffectEvidenceKey(input.Attempt.ID, serving.Boundary.Sequence)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if err != nil {
		return empty, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 {
		return empty, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.Work.ReadRevision {
		return empty, invalid()
	}
	evidence, err := releaseexecution.DecodeEffectEvidence(input, serving.Boundary, value.Value, serving.Evidence)
	if err != nil || evidence.RecordedAt.After(saved.Record.Progress.UpdatedAt) {
		return empty, invalid()
	}
	response := &agentpb.ReleaseHelperResponse{}
	if len(evidence.Result.Helper) == 0 || proto.Unmarshal(evidence.Result.Helper, response) != nil ||
		releaseexecution.ValidateHelperResult(input, serving.Boundary.Progress, response) != nil {
		return empty, invalid()
	}
	runtime, err := executionplan.PrepareReleaseWorkRuntime(input.Attempt, input.Release)
	if err != nil {
		return empty, err
	}
	// The runtime retained after history expires must describe the same routing
	// proven by the final serving observation, including the last rolling step.
	if proxy := response.ProxyEvidence; proxy != nil {
		if proxy.ReleaseId != runtime.ReleaseID || proxy.ServiceId != runtime.ServiceID || proxy.Target != runtime.Target ||
			proxy.ProxyGeneration != runtime.ProxyGeneration || !bytes.Equal(proxy.ConfigSha256, runtime.ProxyConfigSHA256) {
			return empty, invalid()
		}
	} else if runtime.ProxyGeneration != 0 || len(runtime.ProxyConfigSHA256) != 0 {
		return empty, invalid()
	}
	return CompletionProof{Runtime: runtime, Progress: saved, Serving: response, Evidence: evidence, Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(input.Attempt.TaskID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(input.Attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(input.Attempt.ID), ModRevision: view.Attempt.Revision},
		{Key: workqueue.ExecutorKey(input.Attempt.Executor), ModRevision: view.ExecutorRevision},
		{Key: ProgressKey(input.Attempt.ID), ModRevision: saved.Revision},
		{Key: workqueue.CheckpointKey(input.Attempt.ID), ModRevision: view.Checkpoint.Revision},
		{Key: key, ModRevision: value.ModRevision},
	}}, nil
}
