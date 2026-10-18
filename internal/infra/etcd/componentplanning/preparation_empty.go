package componentplanning

func ComponentTaskPreparationIsZero(preparation ComponentTaskPreparation) bool {
	return preparation.Intent.TaskID == "" && preparation.Intent.EnvironmentID == "" &&
		preparation.Intent.Status == "" && len(preparation.Intent.Candidates) == 0 &&
		preparation.Intent.RouteProjection == nil &&
		preparation.Intent.CreatedAt.IsZero() && preparation.Intent.TerminalAt == nil &&
		len(preparation.managedRuntimeSources) == 0 &&
		!preparation.appliedProjectionPresent && preparation.appliedProjectionRevision == 0 &&
		preparation.desiredProjectionRevision == 0 &&
		preparation.unitRuntimeCondition == nil &&
		preparation.proxyServicePreflight == nil && len(preparation.proxyServiceConditions) == 0 &&
		preparation.proxyServiceConditionCount == 0 &&
		len(preparation.addresses) == 0
}
