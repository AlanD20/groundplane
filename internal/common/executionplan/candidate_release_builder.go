package executionplan

import (
	"sort"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// CandidateReleaseProcedureInput is the complete immutable authority for all
// candidate Release mutations in one execution plan.
type CandidateReleaseProcedureInput struct {
	Operation agentpb.PlanOperation
	Members   []CandidateReleaseMemberInput
}

type CandidateReleaseMemberInput struct {
	ServiceID           string
	CandidateReleaseID  string
	CandidateArtifactID string
	ForwardStepIDs      []string
	ServingPredecessor  *ServingPredecessorInput
	CandidateAbsence    *CandidateAbsenceInput
	Rolling             *agentpb.RollingReleaseProcedure
}

type ServingPredecessorInput struct {
	ProbeStepID             string
	CompensateStepID        string
	PriorArtifactID         string
	PriorReleaseID          string
	PriorTarget             string
	RetainedPriorArtifactID string
}

type CandidateAbsenceInput struct {
	ComposeProjectName string
	Services           []CandidateServiceIdentity
	ProbeStepID        string
	CompensateStepID   string
}

type CandidateServiceIdentity struct {
	ServiceID string
	ReleaseID string
}

// BuildCandidateReleaseProcedure returns an owned, canonically ordered
// concrete protobuf value. It does not inspect host or applied state.
func BuildCandidateReleaseProcedure(input CandidateReleaseProcedureInput) (*agentpb.CandidateReleaseProcedure, error) {
	members := append([]CandidateReleaseMemberInput(nil), input.Members...)
	sort.Slice(members, func(left, right int) bool { return members[left].ServiceID < members[right].ServiceID })
	procedure := &agentpb.CandidateReleaseProcedure{Members: make([]*agentpb.CandidateReleaseMember, len(members))}
	for index, inputMember := range members {
		member := &agentpb.CandidateReleaseMember{
			ServiceId: inputMember.ServiceID, CandidateReleaseId: inputMember.CandidateReleaseID,
			CandidateArtifactId: inputMember.CandidateArtifactID,
			ForwardStepIds:      append([]string(nil), inputMember.ForwardStepIDs...),
			Rolling:             proto.CloneOf(inputMember.Rolling),
		}
		if value := inputMember.ServingPredecessor; value != nil {
			member.ServingPredecessor = &agentpb.ServingPredecessorRestoration{
				ProbeStepId: value.ProbeStepID, CompensateStepId: value.CompensateStepID,
				PriorArtifactId: value.PriorArtifactID, PriorReleaseId: value.PriorReleaseID,
				PriorTarget: value.PriorTarget, RetainedPriorArtifactId: value.RetainedPriorArtifactID,
			}
		}
		if value := inputMember.CandidateAbsence; value != nil {
			services := append([]CandidateServiceIdentity(nil), value.Services...)
			sort.Slice(services, func(left, right int) bool {
				if services[left].ServiceID == services[right].ServiceID {
					return services[left].ReleaseID < services[right].ReleaseID
				}
				return services[left].ServiceID < services[right].ServiceID
			})
			member.CandidateAbsence = &agentpb.CandidateAbsenceRestoration{
				ComposeProjectName: value.ComposeProjectName,
				ProbeStepId:        value.ProbeStepID, CompensateStepId: value.CompensateStepID,
				Services: make([]*agentpb.CandidateReleaseService, len(services)),
			}
			for serviceIndex, service := range services {
				member.CandidateAbsence.Services[serviceIndex] = &agentpb.CandidateReleaseService{
					ServiceId: service.ServiceID, ReleaseId: service.ReleaseID,
				}
			}
		}
		procedure.Members[index] = member
	}
	if err := validateCandidateReleaseProcedure(input.Operation, procedure); err != nil {
		return nil, err
	}
	return procedure, nil
}
