package agent

import (
	composeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	"sort"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (state *releaseExecutionState) recordAbsenceResult(result *composeruntime.StepResult, primary error) error {
	if result.CandidateAbsenceEvidence == nil {
		return primary
	}
	if err := state.recordAbsenceEvidence(result.CandidateAbsenceEvidence); err != nil {
		result.ReconciliationRequired = true
		if primary == nil {
			return err
		}
	}
	return primary
}

func (state *releaseExecutionState) recordAbsenceEvidence(evidence *agentpb.CandidateAbsenceEvidence) error {
	if len(evidence.GetCandidates()) != 1 || evidence.Candidates[0].GetServiceId() == "" ||
		evidence.Candidates[0].GetReleaseId() == "" {
		return errs.New(errs.KindInternal, "agent: absence evidence is not member-scoped")
	}
	identity := evidence.Candidates[0].ServiceId
	header := proto.CloneOf(evidence)
	header.Candidates, header.AbsenceProven = nil, false
	for serviceID, prior := range state.absence {
		priorHeader := proto.CloneOf(prior)
		priorHeader.Candidates, priorHeader.AbsenceProven = nil, false
		if !proto.Equal(header, priorHeader) ||
			serviceID == identity && prior.Candidates[0].ReleaseId != evidence.Candidates[0].ReleaseId {
			return errs.New(errs.KindInternal, "agent: absence evidence authority changed")
		}
	}
	if state.absence == nil {
		state.absence = make(map[string]*agentpb.CandidateAbsenceEvidence)
	}
	state.absence[identity] = proto.CloneOf(evidence)
	return nil
}

func (state *releaseExecutionState) aggregateAbsenceEvidence() *agentpb.CandidateAbsenceEvidence {
	if len(state.absence) == 0 {
		return nil
	}
	services := make([]string, 0, len(state.absence))
	for serviceID := range state.absence {
		services = append(services, serviceID)
	}
	sort.Strings(services)
	result := proto.CloneOf(state.absence[services[0]])
	result.Candidates, result.AbsenceProven = nil, true
	for _, serviceID := range services {
		proof := state.absence[serviceID]
		result.Candidates = append(result.Candidates, proto.CloneOf(proof.Candidates[0]))
		result.AbsenceProven = result.AbsenceProven && proof.AbsenceProven
	}
	return result
}
