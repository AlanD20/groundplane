// Package taskcontract owns closed durable Task parameter names shared by
// Controller capabilities and the execution-plan resolver.
package taskcontract

const (
	EnvironmentBlueprintArtifactParam        = "compose_artifact_id"
	EnvironmentBlueprintManagedVolumesParam  = "managed_volume_ids"
	EnvironmentBlueprintProcedureParam       = "blueprint_compose_procedure"
	EnvironmentBlueprintSelectedServiceParam = "blueprint_selected_service"
	EnvironmentRemoveVolumeDirectoryParam    = "remove_volume_dir"
	MaximumBlueprintPostDeployHooks          = 16
)

type BlueprintComposeProcedure string

const (
	BlueprintComposeProcedureNone              BlueprintComposeProcedure = "none"
	BlueprintComposeProcedureFullReconcile     BlueprintComposeProcedure = "full-reconcile"
	BlueprintComposeProcedureCandidateReleases BlueprintComposeProcedure = "candidate-releases"
)

func ParseBlueprintComposeProcedure(value string) (BlueprintComposeProcedure, bool) {
	procedure := BlueprintComposeProcedure(value)
	switch procedure {
	case BlueprintComposeProcedureNone,
		BlueprintComposeProcedureFullReconcile,
		BlueprintComposeProcedureCandidateReleases:
		return procedure, true
	default:
		return "", false
	}
}
