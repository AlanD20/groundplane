package releaserender

import (
	"encoding/hex"
	"math"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// CaptureRollingPlan binds predecessor identities to the acknowledged artifact.
// Scaled recreate additionally supplies exact containers observed before Task
// publication; desired replica names never supply runtime identities.
func CaptureRollingPlan(input *ReleaseRenderInput, timeout uint32, captured []rollingrelease.Replica) error {
	if input == nil {
		return errs.New(errs.KindInternal, "rolling release input is missing")
	}
	if input.Strategy != domain.StrategyRolling {
		return nil
	}
	prior, err := rollingPredecessors(*input, captured)
	if err != nil {
		return err
	}
	plan, err := rollingrelease.NewPlan(input.ServiceID, input.ReleaseID, input.ArtifactID,
		input.CandidateWorkload.ReplicaCount, timeout, prior)
	if err != nil {
		return err
	}
	plan.Retained, err = rollingRetainedPredecessor(*input)
	if err != nil {
		return err
	}
	if err := plan.Validate(); err != nil {
		return err
	}
	input.Rolling = &plan
	return nil
}

// SealRollingProxy advances through every captured traffic change. The render
// record stores the final generation, which becomes the next Release's prior.
func SealRollingProxy(input *ReleaseRenderInput, firstGeneration uint64) error {
	if input == nil || input.Rolling == nil {
		return errs.New(errs.KindInternal, "rolling proxy has no replica plan")
	}
	changes, err := input.Rolling.Changes()
	if err != nil {
		return err
	}
	if firstGeneration == 0 || firstGeneration > math.MaxUint64-uint64(len(changes)-1) {
		return errs.New(errs.KindStateConflict, "rolling proxy generation is exhausted")
	}
	input.ProxyGeneration = firstGeneration + uint64(len(changes)-1)
	config, err := domain.RenderRollingProxyConfig(
		input.ProxyImplementation,
		*input.Rolling,
		uint32(len(changes)),
		input.ProxyGeneration,
		input.ProxyPorts,
	)
	if err != nil {
		return err
	}
	input.ProxyConfigDigest = hex.EncodeToString(config.SHA256[:])
	return nil
}

func rollingPredecessors(
	input ReleaseRenderInput,
	captured []rollingrelease.Replica,
) ([]rollingrelease.Replica, error) {
	if input.PriorRuntime == nil {
		if input.PriorArtifactID != "" || input.PriorWorkload != nil {
			return nil, errs.New(errs.KindStateConflict, "rolling requires the acknowledged predecessor runtime")
		}
		return nil, nil
	}
	if err := validateOrdinaryPriorRuntime(input); err != nil {
		return nil, err
	}
	artifact, err := taskassignments.OpenRestorationWitness(input.EnvironmentID, input.PriorRuntime.CurrentArtifact)
	if err != nil {
		return nil, err
	}
	var replicas []rollingrelease.Replica
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != input.ServiceID ||
			service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		ordinal := service.GetRollingReplicaOrdinal()
		if service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA {
			ordinal = 1
		}
		releaseID := ""
		for _, label := range service.GetExpectedLabels() {
			if label.GetKey() == "com.groundplane.release-id" {
				releaseID = label.GetValue()
			}
		}
		if service.GetExpectedReplicas() != 1 {
			if service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON ||
				len(replicas) != 0 || int(service.GetExpectedReplicas()) != len(captured) || len(captured) < 2 {
				return nil, errs.New(
					errs.KindStateConflict,
					"rolling requires every scaled predecessor container to be captured before publication",
				)
			}
			for index, replica := range captured {
				if replica.ReleaseID != releaseID || replica.ArtifactID != artifact.GetArtifactId() ||
					replica.ComposeName != service.GetComposeName() ||
					replica.Ordinal != uint32(index+1) ||
					replica.ComposeReplica != replica.Ordinal ||
					replica.ContainerID == "" {
					return nil, errs.New(
						errs.KindStateConflict,
						"captured predecessor containers differ from the acknowledged recreate workload",
					)
				}
			}
			replicas = slices.Clone(captured)
			continue
		}
		replicas = append(replicas, rollingrelease.Replica{ReleaseID: releaseID, ArtifactID: artifact.GetArtifactId(),
			ComposeName: service.GetComposeName(), Ordinal: ordinal})
	}
	slices.SortFunc(replicas, func(a, b rollingrelease.Replica) int { return int(a.Ordinal) - int(b.Ordinal) })
	if len(replicas) != int(input.PriorWorkload.ReplicaCount) {
		return nil, errs.New(
			errs.KindStateConflict,
			"rolling predecessor replica count differs from its acknowledged runtime",
		)
	}
	return replicas, nil
}

// A retained slot is not a traffic recipient. It survives until the final
// rolling candidate is healthy, then an exact workload-only step removes it.
// Its original bytes remain in PriorRuntime for failed-rollout restoration.
func rollingRetainedPredecessor(input ReleaseRenderInput) ([]rollingrelease.Replica, error) {
	if input.PriorRuntime == nil || len(input.PriorRuntime.RetainedPriorArtifact) == 0 {
		return nil, nil
	}
	artifact, err := taskassignments.OpenRestorationWitness(
		input.EnvironmentID,
		input.PriorRuntime.RetainedPriorArtifact,
	)
	if err != nil {
		return nil, err
	}
	if input.PriorStrategy != domain.StrategyBlueGreen || len(artifact.GetServices()) != 1 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"rolling retained predecessor is not an inactive blue-green slot",
		)
	}
	service := artifact.GetServices()[0]
	if service.GetServiceId() != input.ServiceID ||
		service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT ||
		service.GetExpectedReplicas() != 1 ||
		service.GetSlot() == string(input.PriorTarget) {
		return nil, errs.New(errs.KindValidationFailed, "rolling retained slot differs from its captured predecessor")
	}
	releaseID := ""
	for _, label := range service.GetExpectedLabels() {
		if label.GetKey() == "com.groundplane.release-id" {
			releaseID = label.GetValue()
		}
	}
	return []rollingrelease.Replica{{ReleaseID: releaseID, ArtifactID: artifact.GetArtifactId(),
		ComposeName: service.GetComposeName(), Ordinal: 1}}, nil
}

func validateRollingRenderInput(input ReleaseRenderInput) error {
	if input.Strategy != domain.StrategyRolling {
		if input.Rolling != nil {
			return errs.New(errs.KindValidationFailed, "non-rolling Release contains a rolling plan")
		}
		return nil
	}
	plan := input.Rolling
	if plan == nil || plan.Validate() != nil || plan.ServiceID != input.ServiceID ||
		plan.ReleaseID != input.ReleaseID ||
		len(plan.Candidate) != int(input.CandidateWorkload.ReplicaCount) ||
		plan.Candidate[0].ArtifactID != input.ArtifactID ||
		input.CandidateTarget != domain.WorkloadRolling ||
		input.Slot != "" {
		return errs.New(errs.KindValidationFailed, "rolling render input does not match its replica plan")
	}
	prior, err := rollingPredecessors(input, plan.Prior)
	if err != nil {
		return err
	}
	if !slices.Equal(prior, plan.Prior) {
		return errs.New(errs.KindValidationFailed, "rolling plan changes the captured predecessor identities")
	}
	retained, err := rollingRetainedPredecessor(input)
	if err != nil {
		return err
	}
	if !slices.Equal(retained, plan.Retained) {
		return errs.New(errs.KindValidationFailed, "rolling plan changes the captured inactive slot identity")
	}
	if len(input.ProxyPorts) == 0 {
		if input.ProxyGeneration != 0 || input.ProxyConfigDigest != "" || input.PriorProxyGeneration != 0 ||
			input.PriorProxyDigest != "" {
			return errs.New(errs.KindValidationFailed, "portless rolling Release contains proxy authority")
		}
		return nil
	}
	changes, err := plan.Changes()
	if err != nil {
		return err
	}
	if input.ProxyGeneration < uint64(len(changes)) ||
		input.ProxyGeneration-uint64(len(changes)-1) <= input.PriorProxyGeneration ||
		(input.PriorProxyGeneration == 0) != (input.PriorProxyDigest == "") ||
		(len(prior) != 0) != (input.PriorProxyGeneration != 0 && input.PriorProxyDigest != "") {
		return errs.New(errs.KindValidationFailed, "rolling render proxy generations are invalid")
	}
	config, err := domain.RenderRollingProxyConfig(
		input.ProxyImplementation,
		*plan,
		uint32(len(changes)),
		input.ProxyGeneration,
		input.ProxyPorts,
	)
	if err != nil {
		return err
	}
	if input.ProxyConfigDigest != hex.EncodeToString(config.SHA256[:]) {
		return errs.New(errs.KindValidationFailed, "rolling render proxy digest differs from the final replica set")
	}
	return nil
}
