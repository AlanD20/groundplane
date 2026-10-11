// Package rollingrelease owns the sealed replica replacement order shared by
// rollout planning and execution. It never reads mutable desired state.
package rollingrelease

import (
	"fmt"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaximumReplicas              = 16
	DefaultReplicaTimeoutSeconds = 300
	MaximumReplicaTimeoutSeconds = 86400
	DrainSeconds                 = 30
)

// Candidates are distinct singleton Compose services. Scaled recreate
// predecessors additionally capture their observed Docker identity; their shared
// Compose service name alone cannot authorize individual routing or retirement.
type Replica struct {
	ReleaseID      string `json:"release_id"`
	ArtifactID     string `json:"artifact_id"`
	ComposeName    string `json:"compose_name"`
	Ordinal        uint32 `json:"ordinal"`
	ContainerID    string `json:"container_id,omitempty"`
	ContainerName  string `json:"container_name,omitempty"`
	ComposeReplica uint32 `json:"compose_replica,omitempty"`
}

type Plan struct {
	ServiceID             string    `json:"service_id"`
	ReleaseID             string    `json:"release_id"`
	ReplicaTimeoutSeconds uint32    `json:"replica_timeout_seconds"`
	Prior                 []Replica `json:"prior,omitempty"`
	Retained              []Replica `json:"retained,omitempty"`
	Candidate             []Replica `json:"candidate"`
}

// Change preserves the exact routing set before and after one replica change.
// A candidate joins After only following its successful health step. A retired
// replica leaves routing before drain/removal; its container still exists then.
type Change struct {
	Ordinal   uint32
	Candidate *Replica
	Retire    *Replica
	Retained  *Replica
	Before    []Replica
	After     []Replica
}

func ReplicaComposeName(releaseID string, ordinal uint32) (string, error) {
	if ids.Validate(ids.KindDeployment, releaseID) != nil || ordinal < 1 || ordinal > MaximumReplicas {
		return "", errs.New(errs.KindValidationFailed, "rolling replica identity is invalid")
	}
	return fmt.Sprintf("gp-%s-r%02d", strings.ToLower(releaseID), ordinal), nil
}

func NewPlan(serviceID, releaseID, artifactID string, replicas, timeoutSeconds uint32, prior []Replica) (Plan, error) {
	if replicas < 1 || replicas > MaximumReplicas {
		return Plan{}, errs.New(errs.KindValidationFailed, "rolling requires 1 to 16 replicas")
	}
	if timeoutSeconds == 0 {
		timeoutSeconds = DefaultReplicaTimeoutSeconds
	}
	plan := Plan{ServiceID: serviceID, ReleaseID: releaseID, ReplicaTimeoutSeconds: timeoutSeconds,
		Prior: slices.Clone(prior), Candidate: make([]Replica, replicas)}
	for index := range plan.Candidate {
		ordinal := uint32(index + 1)
		name, err := ReplicaComposeName(releaseID, ordinal)
		if err != nil {
			return Plan{}, err
		}
		plan.Candidate[index] = Replica{
			ReleaseID:   releaseID,
			ArtifactID:  artifactID,
			ComposeName: name,
			Ordinal:     ordinal,
		}
	}
	return plan, plan.Validate()
}

func (plan Plan) Validate() error {
	if ids.Validate(ids.KindService, plan.ServiceID) != nil ||
		ids.Validate(ids.KindDeployment, plan.ReleaseID) != nil ||
		plan.ReplicaTimeoutSeconds == 0 ||
		plan.ReplicaTimeoutSeconds > MaximumReplicaTimeoutSeconds ||
		len(plan.Prior) > MaximumReplicas ||
		len(plan.Retained) > 1 ||
		len(plan.Retained) != 0 && len(plan.Prior) != 1 ||
		len(plan.Candidate) < 1 ||
		len(plan.Candidate) > MaximumReplicas {
		return errs.New(errs.KindValidationFailed, "rolling plan identity, replica count or timeout is invalid")
	}
	seen := make(map[string]struct{}, len(plan.Prior)+len(plan.Candidate)+len(plan.Retained))
	containers := make(map[string]bool, len(plan.Prior))
	for groupIndex, group := range [][]Replica{plan.Prior, plan.Candidate, plan.Retained} {
		for index, replica := range group {
			if ids.Validate(ids.KindDeployment, replica.ReleaseID) != nil ||
				ids.Validate(ids.KindConfig, replica.ArtifactID) != nil ||
				replica.Ordinal != uint32(index+1) ||
				!validComposeName(replica.ComposeName) ||
				replica.ReleaseID != group[0].ReleaseID ||
				replica.ArtifactID != group[0].ArtifactID {
				return errs.New(
					errs.KindValidationFailed,
					"rolling replicas must have ordered, captured runtime identities",
				)
			}
			if err := replica.validateContainerIdentity(); err != nil {
				return err
			}
			if replica.ContainerID != "" {
				if groupIndex != 0 || len(group) < 2 || len(plan.Retained) != 0 ||
					replica.ComposeName != group[0].ComposeName || replica.ComposeReplica != replica.Ordinal || containers[replica.ContainerID] {
					return errs.New(
						errs.KindValidationFailed,
						"scaled predecessor identities must select every distinct container in order",
					)
				}
				containers[replica.ContainerID] = true
			}
			if (replica.ContainerID != "") != (group[0].ContainerID != "") {
				return errs.New(errs.KindValidationFailed, "rolling predecessor container selection is partial")
			}
			if _, exists := seen[replica.RoutingName()]; exists {
				return errs.New(errs.KindValidationFailed, "rolling replicas must have distinct routing identities")
			}
			seen[replica.RoutingName()] = struct{}{}
			if groupIndex != 1 {
				if replica.ReleaseID == plan.ReleaseID || replica.ArtifactID == plan.Candidate[0].ArtifactID {
					return errs.New(
						errs.KindValidationFailed,
						"rolling predecessor must have distinct Release and artifact identities",
					)
				}
			} else {
				name, err := ReplicaComposeName(plan.ReleaseID, replica.Ordinal)
				if err != nil || replica.ReleaseID != plan.ReleaseID || replica.ComposeName != name {
					return errs.New(errs.KindValidationFailed, "rolling candidate identity is not canonical")
				}
			}
		}
	}
	if len(plan.Retained) != 0 && (plan.Retained[0].ReleaseID == plan.Prior[0].ReleaseID ||
		plan.Retained[0].ArtifactID == plan.Prior[0].ArtifactID) {
		return errs.New(
			errs.KindValidationFailed,
			"retained blue-green slot must have distinct captured Release and artifact identities",
		)
	}
	return nil
}

func validComposeName(value string) bool {
	if len(value) == 0 || len(value) > 63 {
		return false
	}
	for index, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' ||
			index > 0 && (character == '-' || character == '_') {
			continue
		}
		return false
	}
	return true
}

// Changes replaces matching ordinals first, then adds/removes the difference.
// The executable steps never start the next candidate before retiring this
// change's predecessor, so at most one extra candidate exists at a time.
func (plan Plan) Changes() ([]Change, error) {
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	active := slices.Clone(plan.Prior)
	count := max(len(plan.Prior), len(plan.Candidate))
	changes := make([]Change, 0, count)
	for index := range count {
		change := Change{Ordinal: uint32(index + 1), Before: slices.Clone(active)}
		if index < len(plan.Prior) {
			retire := plan.Prior[index]
			change.Retire = &retire
			active = slices.DeleteFunc(active, func(replica Replica) bool { return replica == retire })
		}
		if index < len(plan.Candidate) {
			candidate := plan.Candidate[index]
			change.Candidate = &candidate
			active = append(active, candidate)
		}
		change.After = slices.Clone(active)
		changes = append(changes, change)
	}
	if len(plan.Retained) != 0 {
		retained := plan.Retained[0]
		changes[len(changes)-1].Retained = &retained
	}
	return changes, nil
}

func ClonePlan(plan Plan) Plan {
	plan.Prior = slices.Clone(plan.Prior)
	plan.Retained = slices.Clone(plan.Retained)
	plan.Candidate = slices.Clone(plan.Candidate)
	return plan
}
