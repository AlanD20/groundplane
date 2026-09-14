package servicelifecycle

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type AcknowledgedRuntimeReader interface {
	ReleaseReader
	LoadAcknowledgedServiceRuntimesAtRevision(
		context.Context,
		string,
		[]string,
		int64,
	) ([]etcd.Versioned[serviceruntimerecord.Record], error)
}

// AcknowledgedRuntimeCapture is exact per-Service applied runtime evidence at
// one fixed planning revision. Artifact identity is the only rewritten field.
type AcknowledgedRuntimeCapture struct {
	Release                etcd.ServiceLifecycleRelease
	RuntimeRevision        int64
	CurrentArtifact        []byte
	RetainedPriorArtifact  []byte
	RetainedPriorReleaseID string
}

func CaptureAcknowledgedRuntime(
	ctx context.Context,
	reader AcknowledgedRuntimeReader,
	applied etcd.Versioned[etcd.EnvironmentComposeProjection],
	environmentID string,
	serviceID string,
	currentArtifactID string,
) (AcknowledgedRuntimeCapture, error) {
	if ctx == nil || reader == nil || ids.Validate(ids.KindConfig, currentArtifactID) != nil ||
		applied.ReadRevision <= 0 {
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
	current, _, err := reidentifyRuntimeArtifact(source.Runtime.CurrentArtifact, currentArtifactID, serviceID)
	if err != nil {
		return AcknowledgedRuntimeCapture{}, err
	}
	result := AcknowledgedRuntimeCapture{
		Release: release, RuntimeRevision: sources[0].Revision, CurrentArtifact: current,
	}
	if len(source.Runtime.RetainedPriorArtifact) != 0 {
		retainedID := ids.New(ids.KindConfig)
		result.RetainedPriorArtifact, result.RetainedPriorReleaseID, err = reidentifyRuntimeArtifact(
			source.Runtime.RetainedPriorArtifact,
			retainedID,
			serviceID,
		)
		if err != nil {
			clear(result.CurrentArtifact)
			return AcknowledgedRuntimeCapture{}, err
		}
	}
	if executionplan.ValidateNativePredecessorWitness(
		environmentID,
		serviceID,
		result.CurrentArtifact,
		result.RetainedPriorArtifact,
	) != nil {
		result.Clear()
		return AcknowledgedRuntimeCapture{}, errs.New(
			errs.KindStateConflict,
			"acknowledged runtime artifact identity is invalid",
		)
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

func reidentifyRuntimeArtifact(
	encoded []byte,
	artifactID string,
	serviceID string,
) ([]byte, string, error) {
	artifact := new(agentpb.ComposeArtifact)
	if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, artifact) != nil {
		return nil, "", errs.New(errs.KindStateConflict, "acknowledged runtime artifact is corrupt")
	}
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
		return nil, "", errs.New(errs.KindStateConflict, "acknowledged runtime Release identity is invalid")
	}
	artifact.ArtifactId = artifactID
	value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		return nil, "", errs.Wrap(errs.KindInternal, err)
	}
	return value, releaseID, nil
}
