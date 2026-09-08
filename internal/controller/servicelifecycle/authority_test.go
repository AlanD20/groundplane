package servicelifecycle

import (
	"context"
	"testing"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type authorityReader struct {
	serving   etcd.ServingRelease
	renders   map[string]etcd.Versioned[etcd.ReleaseRenderInput]
	revisions []int64
}

func (reader *authorityReader) ResolveServing(
	_ context.Context, _, _ string, revision int64,
) (etcd.ServingRelease, error) {
	reader.revisions = append(reader.revisions, revision)
	return reader.serving, nil
}

func (reader *authorityReader) GetReleaseRenderInputAt(
	_ context.Context, releaseID string, revision int64,
) (etcd.Versioned[etcd.ReleaseRenderInput], error) {
	reader.revisions = append(reader.revisions, revision)
	return reader.renders[releaseID], nil
}

func TestCaptureReleaseUsesOneAppliedRevisionAndRetainsExactBlueGreenPredecessor(t *testing.T) {
	const (
		revision  = int64(91)
		currentID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		priorID   = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		serviceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		envID     = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	current := etcd.ReleaseRenderInput{
		ReleaseID: currentID, ServiceID: serviceID, EnvironmentID: envID,
		Strategy: domain.StrategyBlueGreen, PriorStrategy: domain.StrategyBlueGreen,
		CandidateTarget: domain.WorkloadGreen, PriorTarget: domain.WorkloadBlue,
	}
	prior := etcd.ReleaseRenderInput{
		ReleaseID: priorID, ServiceID: serviceID, EnvironmentID: envID,
		Strategy: domain.StrategyBlueGreen, CandidateTarget: domain.WorkloadBlue,
	}
	reader := &authorityReader{
		serving: etcd.ServingRelease{
			Intent:             domain.Intent{ID: currentID, PriorServingReleaseID: priorID},
			ProjectionRevision: 81, IntentRevision: 82, Revision: revision,
		},
		renders: map[string]etcd.Versioned[etcd.ReleaseRenderInput]{
			currentID: {Record: current, Revision: 83, ReadRevision: revision},
			priorID:   {Record: prior, Revision: 84, ReadRevision: revision},
		},
	}
	got, err := CaptureRelease(context.Background(), reader,
		etcd.Versioned[etcd.EnvironmentComposeProjection]{ReadRevision: revision}, envID, serviceID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ServingReleaseID != currentID || got.PriorServingReleaseID != priorID ||
		got.RetainedPrior == nil || got.RetainedPrior.ReleaseID != priorID ||
		got.ProjectionRevision != 81 || got.IntentRevision != 82 || got.RenderRevision != 83 ||
		got.RetainedPriorRenderRevision != 84 {
		t.Fatalf("captured authority = %#v", got)
	}
	for _, used := range reader.revisions {
		if used != revision {
			t.Fatalf("authority read at revision %d, want %d", used, revision)
		}
	}
}

func TestCaptureReleaseInitialBlueGreenDoesNotInventInactiveSource(t *testing.T) {
	const (
		revision  = int64(91)
		currentID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		serviceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		envID     = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	current := etcd.ReleaseRenderInput{
		ReleaseID: currentID, ServiceID: serviceID, EnvironmentID: envID,
		Strategy: domain.StrategyBlueGreen, PriorStrategy: domain.StrategyRecreate,
		CandidateTarget: domain.WorkloadBlue, PriorTarget: domain.WorkloadSingleton,
	}
	reader := &authorityReader{
		serving: etcd.ServingRelease{
			Intent: domain.Intent{ID: currentID}, ProjectionRevision: 81, IntentRevision: 82, Revision: revision,
		},
		renders: map[string]etcd.Versioned[etcd.ReleaseRenderInput]{
			currentID: {Record: current, Revision: 83, ReadRevision: revision},
		},
	}
	got, err := CaptureRelease(context.Background(), reader,
		etcd.Versioned[etcd.EnvironmentComposeProjection]{ReadRevision: revision}, envID, serviceID)
	if err != nil {
		t.Fatal(err)
	}
	if got.RetainedPrior != nil || got.PriorServingReleaseID != "" || len(reader.revisions) != 2 {
		t.Fatalf("initial blue-green invented prior authority: %#v reads=%v", got, reader.revisions)
	}
}
