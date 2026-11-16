package releaseexecution

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/healthcheckfailure"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ReadinessFailureProof contains only captured identities and closed observation
// codes. It never includes container output, probe commands or provider errors.
type ReadinessFailureProof struct {
	ObservedAt time.Time                       `json:"observed_at"`
	Container  ReadyContainer                  `json:"container"`
	ExitCode   *int32                          `json:"exit_code,omitempty"`
	Diagnostic agentpb.ComposeHelperDiagnostic `json:"diagnostic,omitempty"`
}

func captureReadinessFailure(destination **ReadinessFailureProof, observed *agentpb.ObservedProject,
	service *agentpb.ComposeService, container *agentpb.ObservedContainer,
) error {
	if destination == nil {
		return nil
	}
	digest, err := readinessLabelsDigest(service.ExpectedLabels)
	if err != nil {
		return err
	}
	proof := &ReadinessFailureProof{ObservedAt: observed.ObservedAt.AsTime().UTC(),
		Container: ReadyContainer{
			ComposeName:    service.ComposeName,
			ContainerID:    container.ContainerId,
			LabelsSHA256:   digest,
			ImageReference: container.ImageReference,
			ImageID:        container.ImageId,
			State:          container.State,
			Health:         container.Health,
		}}
	switch container.State {
	case agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_EXITED,
		agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_DEAD:
		if container.ExitCode != nil {
			code := *container.ExitCode
			proof.ExitCode = &code
		}
	default:
		if container.Health != agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_UNHEALTHY {
			proof.Diagnostic = container.HealthcheckDiagnostic
		}
	}
	*destination = proof
	return nil
}

// FailureResult uses the same readiness decision as execution and admission.
func ReadinessFailureResult(input Input, progress Progress, observed *agentpb.ObservedProject) (EffectResult, error) {
	var failure *ReadinessFailureProof
	_, err := readiness(input, progress, observed, &failure)
	if err == nil || failure == nil {
		return EffectResult{}, invalidProgress()
	}
	return EffectResult{ReadinessFailure: failure}, nil
}

func (proof ReadinessFailureProof) FailureCode() string {
	switch proof.Container.State {
	case agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_EXITED,
		agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_DEAD:
		return "release.candidate_stopped"
	}
	if proof.Container.Health == agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_UNHEALTHY {
		return "release.healthcheck_failed"
	}
	if healthcheckfailure.Detail(proof.Diagnostic) != "" {
		return "release.healthcheck_unavailable"
	}
	return ""
}

func (proof ReadinessFailureProof) Observation() string {
	switch proof.FailureCode() {
	case "release.candidate_stopped":
		if proof.ExitCode != nil {
			return fmt.Sprintf(
				"Container %s stopped before becoming ready (exit code %d). Inspect Service logs.",
				proof.Container.ContainerID,
				*proof.ExitCode,
			)
		}
		return "Container " + proof.Container.ContainerID + " stopped before becoming ready. Inspect Service logs."
	case "release.healthcheck_failed":
		return "Container " + proof.Container.ContainerID + " failed its healthcheck. Inspect Service logs."
	default:
		return healthcheckfailure.Detail(proof.Diagnostic)
	}
}

func (proof ReadinessFailureProof) Error() error {
	return errs.New(errs.KindStateConflict, proof.Observation())
}

func (proof ReadinessFailureProof) validate(input Input, progress Progress, at time.Time) error {
	if proof.ObservedAt.IsZero() || proof.ObservedAt.Before(progress.UpdatedAt) || proof.ObservedAt.After(at) ||
		!proof.ObservedAt.Before(
			input.Attempt.Deadline,
		) || progress.Rolling != nil && !proof.ObservedAt.Before(progress.Rolling.ReplicaDeadline) {
		return invalidProgress()
	}
	services, err := ReadinessWorkloads(input, progress)
	if err != nil {
		return err
	}
	for _, service := range services {
		if service.ComposeName != proof.Container.ComposeName {
			continue
		}
		digest, err := readinessLabelsDigest(service.ExpectedLabels)
		if err != nil || digest != proof.Container.LabelsSHA256 ||
			proof.Container.ImageReference != service.ImageReference ||
			proof.Container.ImageID != service.ImageReference ||
			len(proof.Container.ContainerID) == 0 ||
			len(proof.Container.ContainerID) > 128 {
			return invalidProgress()
		}
		switch proof.FailureCode() {
		case "release.candidate_stopped":
			if proof.Diagnostic != 0 {
				return invalidProgress()
			}
		case "release.healthcheck_failed":
			if !service.HasHealthcheck ||
				proof.Container.State != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING ||
				proof.ExitCode != nil ||
				proof.Diagnostic != 0 {
				return invalidProgress()
			}
		case "release.healthcheck_unavailable":
			if !service.HasHealthcheck ||
				proof.Container.State != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING ||
				proof.ExitCode != nil {
				return invalidProgress()
			}
		default:
			return invalidProgress()
		}
		return nil
	}
	return invalidProgress()
}

func IsReadinessBoundary(progress Progress) bool {
	return progress.InFlight && progress.Phase == Rollout &&
		(progress.Rolling == nil && progress.Action == WaitCandidate || progress.Rolling != nil && progress.Rolling.Phase == rollingrelease.WaitCandidateHealthy)
}

// DiagnosticStage projects only an admitted immutable failure proof. It does not
// infer an action failure from an attempt-level failure or current runtime.
func (proof ReadinessFailureProof) DiagnosticStage(
	attempt workexecution.Attempt,
	progress Progress,
	at time.Time,
) (workexecution.Stage, error) {
	digest, err := hex.DecodeString(proof.Container.LabelsSHA256)
	if !IsReadinessBoundary(progress) || proof.FailureCode() == "" || err != nil || len(digest) != 32 ||
		proof.Container.ComposeName == "" || len(proof.Container.ComposeName) > 128 || proof.Container.ContainerID == "" || len(proof.Container.ContainerID) > 128 ||
		proof.ObservedAt.IsZero() || proof.ObservedAt.Before(progress.UpdatedAt) || proof.ObservedAt.After(at) || attempt.FinishedAt == nil || at.After(*attempt.FinishedAt) ||
		progress.BeganAt == nil || attempt.StartedAt == nil || progress.BeganAt.Before(*attempt.StartedAt) || !proof.ObservedAt.Before(attempt.Deadline) {
		return workexecution.Stage{}, invalidProgress()
	}
	key, name, expected := string(
		Rollout,
	)+"_"+string(
		WaitCandidate,
	), "Check replacement readiness", "Verify the captured readiness requirements before serving traffic"
	if progress.Rolling != nil {
		key = string(
			Rollout,
		) + "_" + strconv.FormatUint(
			uint64(progress.Rolling.Change),
			10,
		) + "_" + string(
			progress.Rolling.Phase,
		)
		name, expected = "Check candidate replica health", "Wait for the configured healthcheck within the replica deadline"
	}
	stage := workexecution.Stage{
		Key:         key,
		Name:        name,
		Expected:    expected,
		State:       workexecution.StageFailed,
		Observed:    proof.Observation(),
		FailureCode: proof.FailureCode(),
		StartedAt:   progress.BeganAt,
		FinishedAt:  &at,
	}
	return stage, stage.Validate()
}
