package releaseexecution

import "github.com/AlanD20/groundplane/proto/agentpb"

// RolloutAction is an operation-owned boundary, not a configurable step list.
// Blue-green preserves its previous healthy slot; recreate removes the captured
// previous workload before starting its replacement. Rolling has its own cursor.
type RolloutAction string

const (
	RemovePrevious         RolloutAction = "remove_previous_workload"
	StartCandidate         RolloutAction = "start_candidate"
	WaitCandidate          RolloutAction = "wait_candidate_ready"
	SwitchTraffic          RolloutAction = "switch_service_traffic"
	RetireSwitchedPrevious RolloutAction = "retire_switched_previous_workload"
	VerifyServing          RolloutAction = "verify_serving_runtime"
)

func firstRolloutAction(input Input) (RolloutAction, error) {
	if input.Release == nil {
		return "", invalidProgress()
	}
	switch input.Release.Strategy {
	case agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE:
		if input.Artifacts.Prior != nil {
			return RemovePrevious, nil
		}
		return StartCandidate, nil
	case agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_BLUE_GREEN:
		return StartCandidate, nil
	default:
		return "", invalidProgress()
	}
}

func validRolloutAction(input Input, action RolloutAction) bool {
	if input.Release == nil {
		return false
	}
	if input.Artifacts.Rolling != nil {
		return action == VerifyServing && input.Release.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_ROLLING
	}
	switch action {
	case RemovePrevious:
		return input.Release.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE && input.Artifacts.Prior != nil
	case StartCandidate, WaitCandidate, VerifyServing:
		return input.Release.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE || input.Release.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_BLUE_GREEN
	case SwitchTraffic:
		return input.Artifacts.Proxy != nil && (input.Release.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE || input.Release.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_BLUE_GREEN)
	case RetireSwitchedPrevious:
		return input.Release.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_BLUE_GREEN && input.Artifacts.Prior != nil &&
			(input.Artifacts.Prior.Target == "singleton" || input.Artifacts.Prior.Target == "rolling")
	default:
		return false
	}
}

func nextRolloutAction(input Input, action RolloutAction) (RolloutAction, error) {
	if !validRolloutAction(input, action) {
		return "", invalidProgress()
	}
	switch action {
	case RemovePrevious:
		return StartCandidate, nil
	case StartCandidate:
		return WaitCandidate, nil
	case WaitCandidate:
		if input.Artifacts.Proxy != nil {
			return SwitchTraffic, nil
		}
		return VerifyServing, nil
	case SwitchTraffic:
		if validRolloutAction(input, RetireSwitchedPrevious) {
			return RetireSwitchedPrevious, nil
		}
		return VerifyServing, nil
	case RetireSwitchedPrevious:
		return VerifyServing, nil
	case VerifyServing:
		return "", nil
	default:
		return "", invalidProgress()
	}
}
