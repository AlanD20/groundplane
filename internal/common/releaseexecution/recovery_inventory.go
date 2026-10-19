package releaseexecution

import (
	"encoding/hex"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// RecoveryInventory records only verified container identities, never Docker
// inspect payloads (which can contain environment secrets). It is an observation,
// not permission to delete these containers or proof that restoration succeeded.
type RecoveryInventory struct {
	CandidateIDs   []string  `json:"candidate_ids"`
	PredecessorIDs []string  `json:"predecessor_ids"`
	ProxyID        string    `json:"proxy_id,omitempty"`
	ObservedAt     time.Time `json:"observed_at"`
}

func (inventory RecoveryInventory) Validate(input RecoveryInput, report RecoveryReport) error {
	if report.Validate(input) != nil || report.Ack == nil || !report.Progress.InFlight ||
		report.Progress.Phase != InspectFailedRuntime || inventory.ObservedAt.IsZero() || inventory.ObservedAt.Location() != time.UTC ||
		inventory.ObservedAt.Before(
			report.Progress.UpdatedAt,
		) || !inventory.ObservedAt.Before(input.Input.Attempt.Deadline) {
		return invalidProgress()
	}
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(input.Input.Attempt, input.Input.Release)
	if err != nil {
		return err
	}
	var candidateLimit, priorLimit uint64
	for _, service := range artifacts.Workloads {
		candidateLimit += uint64(service.ExpectedReplicas)
	}
	if artifacts.Prior != nil {
		for _, service := range artifacts.Prior.Workloads {
			priorLimit += uint64(service.ExpectedReplicas)
		}
		for _, service := range artifacts.Prior.Retained.GetServices() {
			if service != nil && service.ServiceId == input.Input.Release.ServiceId &&
				service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				priorLimit += uint64(service.ExpectedReplicas)
			}
		}
	}
	if uint64(len(inventory.CandidateIDs)) > candidateLimit || uint64(len(inventory.PredecessorIDs)) > priorLimit ||
		!slices.IsSorted(inventory.CandidateIDs) || !slices.IsSorted(inventory.PredecessorIDs) {
		return invalidProgress()
	}
	seen := make(map[string]bool, len(inventory.CandidateIDs)+len(inventory.PredecessorIDs)+1)
	for _, group := range [][]string{inventory.CandidateIDs, inventory.PredecessorIDs} {
		for _, id := range group {
			if !recoveryContainerID(id) || seen[id] {
				return invalidProgress()
			}
			seen[id] = true
		}
	}
	if inventory.ProxyID != "" && (!recoveryContainerID(inventory.ProxyID) || seen[inventory.ProxyID] ||
		artifacts.Proxy == nil && (artifacts.Prior == nil || artifacts.Prior.Proxy == nil)) {
		return invalidProgress()
	}
	return nil
}

func recoveryContainerID(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}
