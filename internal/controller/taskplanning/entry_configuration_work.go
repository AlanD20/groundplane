package taskplanning

import (
	"encoding/hex"
	"slices"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	controllerrevision "github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	fileplanning "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// NewEntryConfigurationWork fixes action identity and exclusion before retaining
// file sources. Publication supplies the captured input and opens the barrier.
func NewEntryConfigurationWork(scope workexecution.Scope, placement workexecution.Placement,
	claim blueprints.EnvironmentBlueprintStageClaim, action workexecution.Action, targetID, targetName string,
) (workexecution.Task, workexecution.Item, error) {
	if blueprints.ValidateEnvironmentBlueprintStageClaim(claim) != nil ||
		claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation ||
		claim.EnvironmentID != scope.EnvironmentID || claim.RevisionID != claim.TaskID ||
		(placement.Executor != workexecution.Agent && (placement.Executor != workexecution.Controller || action != workexecution.ActionRemove)) ||
		(action != workexecution.ActionCreate && action != workexecution.ActionUpdate && action != workexecution.ActionRemove) {
		return workexecution.Task{}, workexecution.Item{}, errs.New(
			errs.KindValidationFailed,
			"Entry action requires its sealed mutation and Agent host",
		)
	}
	task := workexecution.Task{
		ID: claim.TaskID, Actor: workexecution.OperatorActor, Scope: scope, Action: action,
		TargetID: targetID, TargetName: targetName,
		Intent: workexecution.Input{
			ID:     ids.DeriveAt(ids.KindPlan, claim.CreatedAt, claim.TaskID, "request-intent"),
			SHA256: claim.Intent.CiphertextDigest,
		},
		CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
		Deadline: claim.CreatedAt.Add(
			time.Duration(controllerrevision.TaskTimeoutSeconds) * time.Second,
		), ExpectedItems: 1,
	}
	item := workexecution.Item{
		ID: ids.DeriveAt(ids.KindWorkItem, claim.CreatedAt, claim.TaskID, "entry-configuration"), TaskID: task.ID,
		Operation: workexecution.EntryConfiguration, TargetID: scope.EnvironmentID,
		Placement: placement, Phase: workexecution.Waiting, Cleanup: workexecution.CleanupNone,
		Wait:      &workexecution.Wait{Reason: workexecution.WaitInput},
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline,
		Claims: []workexecution.Claim{{ScopeID: scope.EnvironmentID}},
	}
	if placement.Executor == workexecution.Agent {
		item.Claims = append(item.Claims, workexecution.Claim{HostID: placement.HostID, ScopeID: scope.EnvironmentID})
	}
	if err := workexecution.ValidateGraph(task, []workexecution.Item{item}); err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	return task, item, nil
}

type EntryConfigurationRequest struct {
	Task          workexecution.Task
	Item          workexecution.Item
	OperationID   string
	Candidate     projectionrecord.EnvironmentComposeProjection
	Files         []taskmaterialization.Record
	Configuration runtimeconfiguration.WorkCapture
	Secrets       *tasksecretpins.Binding
	Removal       *entrywork.Removal
}

// PrepareConfigurationWork emits a closed operation, not an ExecutionPlan.
// The caller must compare EpochRevision during publication, preventing changes
// to the running-consumer set between its capture and acceptance.
func (runtime EntryMutationRuntime) PrepareConfigurationWork(
	request EntryConfigurationRequest,
) (*agentpb.EntryConfigurationWorkInput, entrywork.Capture, error) {
	task, item, candidate := request.Task, request.Item, request.Candidate
	if runtime.EpochRevision <= 0 || task.Validate() != nil || item.Validate() != nil ||
		item.Operation != workexecution.EntryConfiguration || item.TaskID != task.ID ||
		item.TargetID != candidate.EnvironmentID || task.Scope.EnvironmentID != candidate.EnvironmentID ||
		runtime.Projection.EnvironmentID != candidate.EnvironmentID || candidate.RevisionID != task.ID ||
		candidate.RenderGeneration <= runtime.Projection.RenderGeneration ||
		request.Configuration.Matches(
			task,
			item,
		) != nil || request.Configuration.Generation != candidate.RenderGeneration ||
		(request.Removal != nil) != (task.Action == workexecution.ActionRemove) {
		return nil, entrywork.Capture{}, errs.New(
			errs.KindStateConflict,
			"Entry configuration has inconsistent captured authority",
		)
	}
	artifact, err := entryMutationArtifact(candidate)
	if err != nil {
		return nil, entrywork.Capture{}, err
	}
	baseline, err := composerender.MutateEnvironmentEntryArtifact(artifact, candidate,
		composerender.EnvironmentEntryArtifactMutation{
			ArtifactID: ids.DeriveAt(ids.KindConfig, item.CreatedAt, item.ID, "entry-baseline"),
			Entries:    runtime.Projection.Entries,
		})
	if err != nil {
		return nil, entrywork.Capture{}, err
	}
	configuration, err := entryConfigurationReference(request.Configuration.Current)
	if err != nil {
		return nil, entrywork.Capture{}, err
	}
	input := &agentpb.EntryConfigurationWorkInput{
		OperationId: request.OperationID, EnvironmentId: candidate.EnvironmentID,
		RenderGeneration: candidate.RenderGeneration, Removal: request.Removal != nil,
		Baseline: baseline, Candidate: artifact, Configuration: configuration,
	}
	if request.Configuration.Prior != nil {
		input.PriorConfiguration, err = entryConfigurationReference(*request.Configuration.Prior)
		if err != nil {
			return nil, entrywork.Capture{}, err
		}
	}
	for _, file := range request.Files {
		descriptor, err := fileplanning.BuildFileDescriptor(file, artifact.ArtifactId)
		if err != nil {
			return nil, entrywork.Capture{}, err
		}
		input.Files = append(input.Files, descriptor)
	}
	sort.Slice(
		input.Files,
		func(i, j int) bool { return input.Files[i].GetDestination() < input.Files[j].GetDestination() },
	)
	capture := entrywork.Capture{Configuration: request.Configuration}
	capture.Configuration.Destinations = slices.Clone(request.Configuration.Destinations)
	if request.Configuration.Prior != nil {
		prior := *request.Configuration.Prior
		capture.Configuration.Prior = &prior
	}
	if request.Secrets != nil {
		binding := *request.Secrets
		capture.Secrets = &binding
	}
	if request.Removal != nil {
		removal := *request.Removal
		capture.Removal = &removal
	} else {
		running := slices.Clone(runtime.RunningServiceIDs)
		sort.Strings(running)
		input.RunningConsumerIds, err = entryMutationConsumerIDs(runtime.Projection, candidate, artifact, running)
		if err != nil {
			return nil, entrywork.Capture{}, err
		}
		for _, serviceID := range input.RunningConsumerIds {
			found := false
			for _, source := range runtime.Sources {
				if source.Record.Runtime.ServiceID != serviceID {
					continue
				}
				if found || source.Revision <= 0 {
					return nil, entrywork.Capture{}, errs.New(errs.KindStateConflict, "Entry consumer runtime is ambiguous")
				}
				found = true
				consumer := entrywork.Consumer{Previous: source.Record, Revision: source.Revision,
					CurrentArtifactID: ids.DeriveAt(ids.KindConfig, item.CreatedAt, item.ID, serviceID+"/entry-current")}
				consumer.Previous.Runtime.CurrentArtifact = slices.Clone(source.Record.Runtime.CurrentArtifact)
				consumer.Previous.Runtime.RetainedPriorArtifact = slices.Clone(source.Record.Runtime.RetainedPriorArtifact)
				consumer.Previous.Runtime.ProxyConfigSHA256 = slices.Clone(source.Record.Runtime.ProxyConfigSHA256)
				if source.Record.Configuration != nil {
					previous := *source.Record.Configuration
					consumer.Previous.Configuration = &previous
				}
				if len(source.Record.Runtime.RetainedPriorArtifact) != 0 {
					consumer.RetainedArtifactID = ids.DeriveAt(ids.KindConfig, item.CreatedAt, item.ID, serviceID+"/entry-retained")
				}
				capture.Consumers = append(capture.Consumers, consumer)
			}
			if !found {
				return nil, entrywork.Capture{}, errs.New(errs.KindStateConflict, "Entry consumer acknowledged runtime is unavailable")
			}
		}
	}
	if err := executionplan.ValidateEntryConfigurationWorkInput(input); err != nil {
		return nil, entrywork.Capture{}, err
	}
	return input, capture, nil
}

func entryConfigurationReference(
	reference runtimeconfiguration.Reference,
) (*agentpb.WorkConfigurationReference, error) {
	if err := runtimeconfiguration.ValidateReference(reference); err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.WorkConfigurationReference{ConfigurationId: reference.ID, EnvironmentId: reference.EnvironmentID,
		Generation: reference.Generation, Sha256: digest}, nil
}
