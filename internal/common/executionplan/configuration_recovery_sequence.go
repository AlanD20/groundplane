package executionplan

import "github.com/AlanD20/groundplane/proto/agentpb"

// RecoveryStepIDs returns the canonical recovery-only sequence from a validated
// candidate procedure. File restoration runs before native restoration in each
// phase; native compensation reverses member order while file order stays fixed.
func RecoveryStepIDs(procedure *agentpb.CandidateReleaseProcedure) []string {
	if procedure == nil {
		return nil
	}
	files := procedure.GetConfigurationRestoration().GetFiles()
	stepIDs := make([]string, 0, 2*(len(files)+len(procedure.GetMembers())))
	for _, file := range files {
		stepIDs = append(stepIDs, file.GetProbeStepId())
	}
	for _, member := range procedure.GetMembers() {
		probe, _ := restorationStepIDs(member.GetServingPredecessor(), member.GetCandidateAbsence())
		stepIDs = append(stepIDs, probe)
	}
	for _, file := range files {
		stepIDs = append(stepIDs, file.GetCompensateStepId())
	}
	for index := len(procedure.GetMembers()) - 1; index >= 0; index-- {
		member := procedure.GetMembers()[index]
		_, compensate := restorationStepIDs(member.GetServingPredecessor(), member.GetCandidateAbsence())
		stepIDs = append(stepIDs, compensate)
	}
	return stepIDs
}
