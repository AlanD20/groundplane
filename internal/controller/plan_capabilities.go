package controller

import "github.com/AlanD20/groundplane/pkg/errs"

func (resolver *TaskPlanResolver) EnableRoutePlans(state routeProviderStateReader) error {
	if resolver == nil || state == nil {
		return errs.New(errs.KindInternal, "Route plan state reader is required")
	}
	resolver.routeState = state
	return nil
}

func (resolver *TaskPlanResolver) EnableVolumeRemovalPlans(reader volumeRemovalPlanReader) error {
	if resolver == nil || reader == nil {
		return errs.New(errs.KindInternal, "Volume removal plan evidence is required")
	}
	resolver.volumeRemovalPlans = reader
	return nil
}
