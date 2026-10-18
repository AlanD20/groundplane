package releaseexecution

import (
	"bytes"
	"crypto/sha256"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type TrafficChange struct {
	Before       []byte
	After        []byte
	BeforeSHA256 [32]byte
	AfterSHA256  [32]byte
	Generation   uint64
	Target       string
}

func (progress Progress) ValidateTrafficEvidence(input Input, evidence *agentpb.ServiceProxyEvidence) error {
	traffic, err := progress.Traffic(input)
	if err != nil {
		return err
	}
	if evidence == nil || executionplan.RejectUnknown(evidence) != nil || evidence.ServiceId != input.Release.ServiceId ||
		evidence.ReleaseId != input.Release.ReleaseId || evidence.Target != traffic.Target || evidence.ProxyGeneration != traffic.Generation ||
		evidence.Compensated || !bytes.Equal(evidence.ConfigSha256, traffic.AfterSHA256[:]) {
		return invalidProgress()
	}
	return nil
}

// Traffic selects the complete before/after routing captured for this boundary.
// It cannot discover a current configuration or infer upstreams from live names.
func (progress Progress) Traffic(input Input) (TrafficChange, error) {
	if progress.Validate(input) != nil || !progress.InFlight || progress.Phase != Rollout {
		return TrafficChange{}, invalidProgress()
	}
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(input.Attempt, input.Release)
	if err != nil {
		return TrafficChange{}, err
	}
	if artifacts.Proxy == nil {
		return TrafficChange{}, invalidProgress()
	}
	index := 0
	target := artifacts.Workloads[0].Slot
	if target == "" {
		target = "singleton"
	}
	if progress.Rolling != nil {
		if progress.Rolling.Phase != rollingrelease.SwitchTraffic && progress.Rolling.Phase != rollingrelease.DrainPredecessor &&
			progress.Rolling.Phase != rollingrelease.RemovePredecessor && progress.Rolling.Phase != rollingrelease.RemoveRetained {
			return TrafficChange{}, invalidProgress()
		}
		index, target = int(progress.Rolling.Change)-1, "rolling"
	} else if progress.Action != SwitchTraffic && progress.Action != VerifyServing {
		return TrafficChange{}, invalidProgress()
	} else if artifacts.Rolling != nil {
		index, target = len(input.Release.TrafficConfigurations)-1, "rolling"
	}
	if index < 0 || index >= len(input.Release.TrafficConfigurations) {
		return TrafficChange{}, invalidProgress()
	}
	before := artifacts.Proxy.ProxyConfigJson
	if artifacts.Prior != nil && artifacts.Prior.Proxy != nil {
		before = artifacts.Prior.Proxy.ProxyConfigJson
	}
	if index > 0 {
		before = input.Release.TrafficConfigurations[index-1]
	}
	after := input.Release.TrafficConfigurations[index]
	implementation, err := executionplan.OpenProxyImplementation(artifacts.Proxy.ProxyImplementation)
	if err != nil {
		return TrafficChange{}, err
	}
	configuration, err := serviceproxy.Read(implementation, after)
	if err != nil {
		return TrafficChange{}, err
	}
	return TrafficChange{Before: slices.Clone(before), After: slices.Clone(after), BeforeSHA256: sha256.Sum256(before), AfterSHA256: sha256.Sum256(after), Generation: configuration.Generation, Target: target}, nil
}
