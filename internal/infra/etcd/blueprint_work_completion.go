package etcd

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BlueprintReleaseCompletion struct {
	PublicationID string
	Proof         releasework.CompletionProof
	Configuration runtimeconfiguration.WorkCapture
	Predecessor   BlueprintNativePredecessor
}

// LoadBlueprintReleaseCompletion reads completed execution and immutable source
// authority at one revision. It deliberately does not require the Blueprint to
// remain the latest: an already-authorized effect must still be recorded after
// supersession. Settlement decides the work outcome separately and must join
// Proof.Conditions with the resource writes and queue completion transaction.
func (ledger *ReleaseLedger) LoadBlueprintReleaseCompletion(ctx context.Context, view workqueue.ResumeView, input releaseexecution.Input) (BlueprintReleaseCompletion, error) {
	var empty BlueprintReleaseCompletion
	if ledger == nil || ledger.store == nil || input.Release == nil || input.Attempt.Direction != workexecution.Forward ||
		!workexecution.SameAssignment(input.Attempt, view.Attempt.Attempt) {
		return empty, errs.New(errs.KindStateConflict, "Release completion requires its captured forward attempt")
	}
	sources, err := ledger.LoadReleaseWorkInput(ctx, view.Work)
	if err != nil {
		return empty, err
	}
	defer clear(sources.Rollout.Record.CandidateArtifact)
	if len(sources.Predecessors) != 1 || sources.Predecessors[0].ServiceID != input.Release.ServiceId {
		return empty, errs.New(errs.KindStateConflict, "Release completion requires its exact Service predecessor")
	}
	render := sources.Rollout.Record.Render
	configuration := sources.Input.Configuration.Current
	selected := input.Release.Configuration
	if input.Release.ServiceId != render.ServiceID || input.Release.ReleaseId != render.ReleaseID ||
		input.Release.EnvironmentId != sources.Rollout.Record.Scope.EnvironmentID ||
		input.Release.OperationId != sources.Input.OperationID || input.Release.RenderGeneration != render.Projection.RenderGeneration ||
		selected == nil || selected.ConfigurationId != configuration.ID || selected.EnvironmentId != configuration.EnvironmentID ||
		selected.Generation != configuration.Generation || hex.EncodeToString(selected.Sha256) != configuration.SHA256 {
		return empty, errs.New(errs.KindStateConflict, "Release completion differs from its published input")
	}
	proof, err := releasework.LoadCompletionProof(ctx, ledger.store, view, input)
	if err != nil {
		return empty, err
	}
	proof.Conditions = append(proof.Conditions, sources.Conditions...)
	return BlueprintReleaseCompletion{PublicationID: sources.Rollout.Record.PublicationID, Proof: proof,
		Configuration: sources.Input.Configuration, Predecessor: sources.Predecessors[0]}, nil
}

// Runtime publication is private to the joined settlement transaction.
func prepareBlueprintReleaseRuntime(input releaseexecution.Input, completion BlueprintReleaseCompletion, at time.Time) (workqueue.Change, error) {
	runtime, err := serviceruntimerecord.AcknowledgeCompletedRelease(input, completion.Proof, completion.Configuration.Current, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	value, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", runtime)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := serviceruntimerecord.Key(runtime.Runtime.ServiceID)
	conditions := append(completion.Proof.Conditions, keyvalue.Condition{Key: key, ModRevision: completion.Predecessor.RuntimeRevision})
	return workqueue.Change{Conditions: conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: key, Value: value},
	}}, nil
}
