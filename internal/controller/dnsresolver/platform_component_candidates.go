package dnsresolver

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func platformComponentLifecycleCandidate(
	desired core.Component,
	action string,
) (core.Component, bool, bool, error) {
	ensureService := false
	disableService := false
	switch action {
	case "enable":
		if desired.Config.CoreDNS == nil {
			return core.Component{}, false, false, errs.New(
				errs.KindStateConflict,
				"CoreDNS must be configured before enable",
			)
		}
		desired.Enabled = true
		ensureService = true
	case "disable":
		if !desired.Enabled {
			return core.Component{}, false, false, errs.New(
				errs.KindStateConflict,
				"CoreDNS is already disabled",
			)
		}
		desired.Enabled = false
		disableService = true
	case "update":
		if !desired.Enabled || desired.Config.CoreDNS == nil {
			return core.Component{}, false, false, errs.New(
				errs.KindStateConflict,
				"CoreDNS must be enabled and configured before update",
			)
		}
		ensureService = true
	default:
		return core.Component{}, false, false, errs.New(errs.KindInternal, "CoreDNS lifecycle action is invalid")
	}
	return desired, ensureService, disableService, nil
}

func platformComponentConfigCandidate(
	desired core.Component,
	config core.CoreDNSComponentConfig,
) core.Component {
	desired.Config = core.ComponentConfig{CoreDNS: &config}
	return desired
}
