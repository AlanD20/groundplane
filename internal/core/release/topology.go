package release

import (
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// WorkloadTarget is physical runtime topology. It is deliberately separate
// from Slot: recreate Releases never acquire durable slot state.
type WorkloadTarget string

const (
	WorkloadSingleton WorkloadTarget = "singleton"
	WorkloadBlue      WorkloadTarget = "blue"
	WorkloadGreen     WorkloadTarget = "green"
)

// TopologyTransition closes the strategy-change decisions sealed into a
// Release render input. Re-evaluating it after restart is pure and therefore
// yields the same cleanup authority without consulting live desired state.
type TopologyTransition struct {
	CandidateStrategy Strategy
	CandidateTarget   WorkloadTarget
	PriorStrategy     Strategy
	PriorTarget       WorkloadTarget
}

func NewTopologyTransition(
	candidateStrategy Strategy,
	candidateSlot Slot,
	priorStrategy Strategy,
	priorSlot Slot,
) (TopologyTransition, error) {
	candidateTarget, err := TargetFor(candidateStrategy, candidateSlot)
	if err != nil {
		return TopologyTransition{}, err
	}
	priorTarget, err := TargetFor(priorStrategy, priorSlot)
	if err != nil {
		return TopologyTransition{}, err
	}
	return TopologyTransition{
		CandidateStrategy: candidateStrategy, CandidateTarget: candidateTarget,
		PriorStrategy: priorStrategy, PriorTarget: priorTarget,
	}, nil
}

func (transition TopologyTransition) RequiresPriorArtifact() bool {
	return transition.CandidateStrategy == StrategyRecreate || transition.CandidateStrategy != transition.PriorStrategy
}

func (transition TopologyTransition) RemovePriorBeforeApply() bool {
	return transition.CandidateStrategy == StrategyRecreate
}

func (transition TopologyTransition) RemovePriorAfterSwitch() bool {
	return transition.CandidateStrategy == StrategyBlueGreen && transition.PriorStrategy == StrategyRecreate
}

func (transition TopologyTransition) RestorePriorDuringCompensation() bool {
	return transition.RequiresPriorArtifact()
}

func TargetFor(strategy Strategy, slot Slot) (WorkloadTarget, error) {
	switch strategy {
	case StrategyRecreate:
		if slot != "" {
			return "", errs.New(errs.KindValidationFailed, "recreate workload target cannot have a slot")
		}
		return WorkloadSingleton, nil
	case StrategyBlueGreen:
		switch slot {
		case SlotBlue:
			return WorkloadBlue, nil
		case SlotGreen:
			return WorkloadGreen, nil
		default:
			return "", errs.New(errs.KindValidationFailed, "blue-green workload target requires a slot")
		}
	default:
		return "", errs.New(errs.KindValidationFailed, "workload target strategy is invalid")
	}
}

func (target WorkloadTarget) Validate() error {
	switch target {
	case WorkloadSingleton, WorkloadBlue, WorkloadGreen:
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "workload target is invalid")
	}
}

func (target WorkloadTarget) Slot() Slot {
	switch target {
	case WorkloadBlue:
		return SlotBlue
	case WorkloadGreen:
		return SlotGreen
	default:
		return ""
	}
}

func WorkloadComposeName(serviceName string, target WorkloadTarget) (string, error) {
	if serviceName == "" || strings.ContainsAny(serviceName, "/\\\x00") || target.Validate() != nil {
		return "", errs.New(errs.KindValidationFailed, "workload Compose identity is invalid")
	}
	return serviceName + "--" + string(target), nil
}
