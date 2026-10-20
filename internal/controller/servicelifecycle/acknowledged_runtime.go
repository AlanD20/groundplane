package servicelifecycle

import (
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"

	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type AcknowledgedRuntimeReader interface {
	ReleaseReader
	LoadAcknowledgedServiceRuntimesAtRevision(
		context.Context,
		string,
		[]string,
		int64,
	) ([]etcdstore.Versioned[serviceruntimerecord.Record], error)
}

// AcknowledgedRuntimeCapture is exact per-Service applied runtime evidence at
// one fixed planning revision. Neither artifact bytes nor identity are rewritten.
type AcknowledgedRuntimeCapture struct {
	Release                releaserender.ServiceLifecycleRelease
	RuntimeRevision        int64
	CurrentArtifact        []byte
	CurrentArtifactID      string
	RetainedPriorArtifact  []byte
	RetainedPriorReleaseID string
}

func CaptureAcknowledgedRuntime(
	ctx context.Context,
	reader AcknowledgedRuntimeReader,
	applied etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	environmentID string,
	serviceID string,
) (AcknowledgedRuntimeCapture, error) {
	if ctx == nil || reader == nil || applied.ReadRevision <= 0 {
		return AcknowledgedRuntimeCapture{}, errs.New(
			errs.KindValidationFailed,
			"acknowledged runtime capture input is invalid",
		)
	}
	release, err := CaptureRelease(ctx, reader, applied, environmentID, serviceID)
	if err != nil {
		return AcknowledgedRuntimeCapture{}, err
	}
	sources, err := reader.LoadAcknowledgedServiceRuntimesAtRevision(
		ctx,
		environmentID,
		[]string{serviceID},
		applied.ReadRevision,
	)
	if err != nil {
		return AcknowledgedRuntimeCapture{}, err
	}
	if len(sources) != 1 || sources[0].Revision <= 0 || sources[0].Revision > applied.ReadRevision ||
		sources[0].ReadRevision != applied.ReadRevision {
		return AcknowledgedRuntimeCapture{}, errs.New(
			errs.KindStateConflict,
			"acknowledged runtime capture is incomplete",
		)
	}
	source := sources[0].Record
	if serviceruntimerecord.Validate(source) != nil || source.EnvironmentID != environmentID ||
		source.Runtime.ServiceID != serviceID || source.Runtime.ReleaseID != release.ServingReleaseID ||
		source.Runtime.Target != string(release.Current.CandidateTarget) {
		return AcknowledgedRuntimeCapture{}, errs.New(
			errs.KindStateConflict,
			"acknowledged runtime does not match serving authority",
		)
	}
	runtime, err := executionplan.OpenNativeServingRuntime(environmentID, serviceID,
		source.Runtime.CurrentArtifact, source.Runtime.RetainedPriorArtifact)
	if err != nil {
		return AcknowledgedRuntimeCapture{}, err
	}
	result := AcknowledgedRuntimeCapture{
		Release: release, RuntimeRevision: sources[0].Revision,
		CurrentArtifact: slices.Clone(
			source.Runtime.CurrentArtifact,
		), CurrentArtifactID: runtime.Artifact.GetArtifactId(),
	}
	if len(source.Runtime.RetainedPriorArtifact) != 0 {
		result.RetainedPriorArtifact = slices.Clone(source.Runtime.RetainedPriorArtifact)
		result.RetainedPriorReleaseID, err = runtimeArtifactReleaseID(runtime.Retained, serviceID)
		if err != nil {
			result.Clear()
			return AcknowledgedRuntimeCapture{}, err
		}
	}
	return result, nil
}

func (capture *AcknowledgedRuntimeCapture) Clear() {
	if capture == nil {
		return
	}
	clear(capture.CurrentArtifact)
	clear(capture.RetainedPriorArtifact)
	*capture = AcknowledgedRuntimeCapture{}
}

func runtimeArtifactReleaseID(
	artifact *agentpb.ComposeArtifact,
	serviceID string,
) (string, error) {
	releaseID := ""
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID ||
			service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		for _, label := range service.GetExpectedLabels() {
			if label.GetKey() == "com.groundplane.release-id" {
				releaseID = label.GetValue()
			}
		}
	}
	if ids.Validate(ids.KindDeployment, releaseID) != nil {
		return "", errs.New(errs.KindStateConflict, "acknowledged runtime Release identity is invalid")
	}
	return releaseID, nil
}
