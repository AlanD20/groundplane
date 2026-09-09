package controller

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// EntryMutationRuntime captures the runtime onto which Entry decorations are
// applied. The epoch fences its selection in the desired publication.
type EntryMutationRuntime struct {
	Projection    etcd.EnvironmentComposeProjection
	EpochRevision int64
}

func (resolver *TaskPlanResolver) CaptureEntryMutationRuntime(
	ctx context.Context, current etcd.Versioned[etcd.EnvironmentComposeProjection],
) (EntryMutationRuntime, error) {
	if resolver == nil || resolver.releases == nil {
		return EntryMutationRuntime{}, errs.New(errs.KindInternal, "Entry runtime capture is not configured")
	}
	scope, err := resolver.releases.LoadPlanningScopeAtRevision(ctx, current.Record.EnvironmentID, current.ReadRevision)
	if err != nil {
		return EntryMutationRuntime{}, err
	}
	if scope.Compose.Revision != current.Revision || scope.Compose.Record.RevisionID != current.Record.RevisionID ||
		!bytes.Equal(scope.Compose.Record.ComposeArtifact, current.Record.ComposeArtifact) {
		return EntryMutationRuntime{}, errs.New(errs.KindStateConflict, "Entry desired runtime source changed")
	}
	baseline, err := entryMutationArtifact(current.Record)
	if err != nil {
		return EntryMutationRuntime{}, err
	}
	var sources []*agentpb.ComposeArtifact
	var retained []string
	for _, desired := range current.Record.DesiredServices {
		planning, err := resolver.releases.LoadPlanningServices(ctx, scope, []string{desired.Desired.ID})
		if err != nil {
			return EntryMutationRuntime{}, err
		}
		if planning[0].Projection.ServingReleaseID == "" {
			continue
		}
		captured, err := servicelifecycle.CaptureRelease(ctx, resolver.releases,
			etcd.Versioned[etcd.EnvironmentComposeProjection]{ReadRevision: scope.ReadRevision},
			current.Record.EnvironmentID, desired.Desired.ID)
		if err != nil {
			return EntryMutationRuntime{}, err
		}
		artifacts, err := resolver.RenderRetainedServiceRuntime(ctx, captured)
		if err != nil {
			return EntryMutationRuntime{}, err
		}
		for index, artifact := range artifacts {
			source := captured.Current.Projection
			if index > 0 {
				source = captured.RetainedPrior.Projection
			}
			// Remove historical Entry decorations before attaching the current
			// generations. A deleted file must not return from Release history.
			artifact, err = mutateEnvironmentEntryArtifact(artifact, source,
				EnvironmentEntryArtifactMutation{ArtifactID: artifact.ArtifactId})
			if err != nil {
				return EntryMutationRuntime{}, err
			}
			sources = append(sources, artifact)
		}
		retained = append(retained, desired.Desired.ID)
	}
	if len(retained) > 0 {
		baseline, err = RetainBlueprintNativeRuntimeSources(baseline, sources, retained)
		if err != nil {
			return EntryMutationRuntime{}, err
		}
	}
	baseline, err = mutateEnvironmentEntryArtifact(baseline, current.Record,
		EnvironmentEntryArtifactMutation{ArtifactID: baseline.ArtifactId, Entries: current.Record.Entries})
	if err != nil {
		return EntryMutationRuntime{}, err
	}
	projection := current.Record
	projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(baseline)
	if err != nil {
		return EntryMutationRuntime{}, errs.Wrap(errs.KindInternal, err)
	}
	return EntryMutationRuntime{Projection: projection, EpochRevision: scope.EnvironmentEpochRevision}, nil
}
