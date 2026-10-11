package blueprint

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/internal/common/zonedns"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

type authoredUnitPlanner struct {
	input          authoredParentInput
	project        *composetypes.Project
	identities     map[ids.Kind]map[string]projectionrecord.OwnedIdentity
	targets        map[blueprintunits.ResourceKey]authoredOwnedUnitTarget
	units          map[blueprintunits.ResourceKey]blueprintunits.Unit
	blocked        map[blueprintunits.ResourceKey]bool
	runtimeReady   bool
	serviceProxies map[string]serviceproxy.Implementation
	attachInputs   map[string]attachinputs.Generation
	appliedAttach  map[string]blueprintunits.AppliedRecord
}

func newAuthoredUnitPlanner(
	ctx context.Context,
	input authoredParentInput,
	targets []authoredOwnedUnitTarget,
	runtime *projectionrecord.EnvironmentComposeProjection,
	attachInputs map[string]attachinputs.Generation,
	snapshot blueprintunits.Snapshot,
) (*authoredUnitPlanner, error) {
	project, err := composerender.LoadNormalizedEnvironmentDesiredProject(
		ctx,
		input.desired,
		input.identities,
	)
	if err != nil {
		return nil, err
	}
	planner := &authoredUnitPlanner{
		input: input, project: project,
		identities:     make(map[ids.Kind]map[string]projectionrecord.OwnedIdentity),
		targets:        make(map[blueprintunits.ResourceKey]authoredOwnedUnitTarget, len(targets)),
		units:          make(map[blueprintunits.ResourceKey]blueprintunits.Unit, len(targets)),
		blocked:        make(map[blueprintunits.ResourceKey]bool),
		runtimeReady:   runtime != nil,
		serviceProxies: make(map[string]serviceproxy.Implementation),
		attachInputs:   attachInputs,
		appliedAttach:  make(map[string]blueprintunits.AppliedRecord),
	}
	for _, applied := range snapshot.Applied {
		if applied.Record.Target.Kind == ids.KindAttach {
			planner.appliedAttach[applied.Record.Target.ID] = applied.Record
		}
	}
	for _, group := range []struct {
		kind   ids.Kind
		values []projectionrecord.OwnedIdentity
	}{
		{ids.KindService, input.identities.Services},
		{ids.KindNetwork, input.identities.Networks},
		{ids.KindVolume, input.identities.Volumes},
		{ids.KindEnvEntry, input.identities.Entries},
		{ids.KindRoute, input.identities.Routes},
		{ids.KindDNSRecord, input.identities.DNSRecords},
		{ids.KindAttach, input.identities.Attaches},
		{ids.KindComponent, input.identities.Components},
		{ids.KindScript, input.identities.Scripts},
	} {
		planner.identities[group.kind] = make(map[string]projectionrecord.OwnedIdentity, len(group.values))
		for _, identity := range group.values {
			planner.identities[group.kind][identity.Name] = identity
		}
	}
	for _, target := range targets {
		planner.targets[target.Target] = target
	}
	if err := planner.validateNamedIdentityCoverage(); err != nil {
		return nil, err
	}
	if runtime != nil {
		if err := planner.bindServiceProxySelections(*runtime); err != nil {
			return nil, err
		}
	}
	return planner, nil
}

func (planner *authoredUnitPlanner) validateNamedIdentityCoverage() error {
	for _, check := range []struct {
		label    string
		authored []string
		kind     ids.Kind
	}{
		{"Attach", sortedAuthoredMapKeys(planner.input.desired.Input.Attachments), ids.KindAttach},
		{"Component", sortedAuthoredMapKeys(planner.input.desired.Input.Components), ids.KindComponent},
		{"Script", sortedAuthoredMapKeys(planner.input.desired.Input.Scripts), ids.KindScript},
		{"DNS", sortedAuthoredMapKeys(planner.input.desired.Input.DNS), ids.KindDNSRecord},
	} {
		stored := planner.identities[check.kind]
		if len(check.authored) != len(stored) {
			return errs.Newf(errs.KindStateConflict, "Blueprint %s desired identities changed", check.label)
		}
		for _, name := range check.authored {
			if _, found := stored[name]; !found {
				return errs.Newf(errs.KindStateConflict, "Blueprint %s desired identity is missing", check.label)
			}
		}
	}
	return nil
}

func sortedAuthoredMapKeys[T any](values map[string]T) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	slices.Sort(result)
	return result
}

func (planner *authoredUnitPlanner) plan() ([]blueprintunits.Unit, bool, error) {
	complete := true
	for name, identity := range planner.identities[ids.KindNetwork] {
		config, found := planner.project.Networks[name]
		if !found || bool(config.External) {
			return nil, false, planner.missingInput("Network", name)
		}
		fingerprint, err := authoredUnitFingerprint("network", struct {
			Name string
			Pool string
			Spec composetypes.NetworkConfig
		}{name, planner.input.desired.Input.NetworkPool, config})
		if err != nil {
			return nil, false, err
		}
		planner.addUnit(identity, fingerprint, nil, nil)
	}
	for name, identity := range planner.identities[ids.KindVolume] {
		config, found := planner.project.Volumes[name]
		if !found || bool(config.External) {
			return nil, false, planner.missingInput("Volume", name)
		}
		fingerprint, err := authoredUnitFingerprint("volume", struct {
			Name  string
			Slug  string
			Spec  composetypes.VolumeConfig
			Files []core.BlueprintFile
		}{name, identity.Slug, config, planner.volumeRuntimeFiles(config)})
		if err != nil {
			return nil, false, err
		}
		planner.addUnit(identity, fingerprint, nil, nil)
	}
	for name, identity := range planner.identities[ids.KindEnvEntry] {
		spec, found := planner.input.desired.Input.Entries[name]
		if !found {
			return nil, false, planner.missingInput("Entry", name)
		}
		if spec.Source.Fact != nil {
			attach, found := planner.identities[ids.KindAttach][spec.Source.Fact.Attach]
			if !found {
				return nil, false, planner.missingInput("Entry fact Attach", spec.Source.Fact.Attach)
			}
			generation, hasInput := planner.attachInputs[attach.ID]
			applied, hasApplied := planner.appliedAttach[attach.ID]
			if !hasInput || !hasApplied || applied.State != blueprintunits.Applied ||
				generation.OwnerKind != attachinputs.OwnerChild ||
				generation.OwnerTaskID != applied.SourceTaskID {
				planner.block(identity)
				complete = false
				continue
			}
		}
		reads, err := planner.entryReads(spec)
		if err != nil {
			return nil, false, err
		}
		fingerprint, err := authoredUnitFingerprint("entry", struct {
			Name              string
			ValueGenerationID string
			Spec              core.EntrySpec
		}{name, identity.ValueGenerationID, spec})
		if err != nil {
			return nil, false, err
		}
		planner.addUnit(identity, fingerprint, reads, nil)
	}
	for name, identity := range planner.identities[ids.KindAttach] {
		generation, found := planner.attachInputs[identity.ID]
		if !found {
			planner.block(identity)
			complete = false
			continue
		}
		spec, found := planner.input.desired.Input.Attachments[name]
		if !found || generation.AttachName != name || generation.ConsumerServiceID == "" {
			return nil, false, planner.missingInput("Attach", name)
		}
		authoredSpecSHA256, err := authoredUnitFingerprint("attach-spec", spec)
		if err != nil {
			return nil, false, err
		}
		if generation.AuthoredSpecSHA256 != authoredSpecSHA256 {
			return nil, false, errs.New(errs.KindStateConflict, "Blueprint Attach authored specification changed")
		}
		consumer, found := planner.identities[ids.KindService][spec.Service]
		if !found || consumer.ID != generation.ConsumerServiceID {
			return nil, false, planner.missingInput("Attach consumer Service", spec.Service)
		}
		fingerprintAuthority := generation
		fingerprintAuthority.OwnerKind = ""
		fingerprintAuthority.OwnerTaskID = ""
		fingerprintAuthority.Transfer = nil
		fingerprint, err := authoredUnitFingerprint("attach", fingerprintAuthority)
		if err != nil {
			return nil, false, err
		}
		planner.addUnit(identity, fingerprint, nil, nil)
	}
	serviceBlocked := make(map[string]bool, len(planner.identities[ids.KindService]))
	// Service children share one complete Environment runtime projection.
	// Independent Networks and Volumes remain selectable while it is pending.
	if !planner.runtimeReady {
		complete = false
		for name := range planner.identities[ids.KindService] {
			serviceBlocked[name] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for name := range planner.identities[ids.KindService] {
			if serviceBlocked[name] {
				continue
			}
			service, found := planner.service(name)
			if !found {
				return nil, false, planner.missingInput("Service", name)
			}
			for dependency := range service.DependsOn {
				if serviceBlocked[dependency] {
					serviceBlocked[name], changed = true, true
					break
				}
			}
			if extension := planner.input.desired.Input.ServiceExtensions[name]; !serviceBlocked[name] {
				for dependency := range extension.DependsOn {
					if serviceBlocked[dependency] {
						serviceBlocked[name], changed = true, true
						break
					}
				}
			}
		}
	}
	for name, identity := range planner.identities[ids.KindService] {
		if serviceBlocked[name] {
			planner.block(identity)
			complete = false
			continue
		}
		service, found := planner.service(name)
		if !found {
			return nil, false, planner.missingInput("Service", name)
		}
		reads, after, err := planner.serviceDependencies(name, service)
		if err != nil {
			return nil, false, err
		}
		dnsUpstreams, err := zonedns.Upstreams(service, planner.project.Networks)
		if err != nil {
			return nil, false, err
		}
		fingerprint, err := authoredUnitFingerprint("service", struct {
			Name                string
			Spec                composetypes.ServiceConfig
			Extension           core.ServiceExtensionSpec
			Files               []core.BlueprintFile
			Entries             []string
			Attaches            map[string]core.AttachmentSpec
			ReleaseGroups       map[string]core.ReleaseGroupSpec
			Scripts             map[string]core.ScriptSpec
			DNSUpstreams        []string
			ProxyImplementation serviceproxy.Implementation
		}{
			name, service, planner.input.desired.Input.ServiceExtensions[name],
			planner.serviceRuntimeFiles(service), planner.serviceEntryNames(name),
			planner.serviceAttachments(name), planner.serviceReleaseGroups(name),
			planner.serviceScripts(name),
			dnsUpstreams, planner.serviceProxies[name],
		})
		if err != nil {
			return nil, false, err
		}
		planner.addUnit(identity, fingerprint, reads, after)
	}
	for index, spec := range planner.input.desired.Input.Routes {
		name := projectionrecord.RouteIdentityName(spec.Hostname, spec.Path)
		identity, found := planner.identities[ids.KindRoute][name]
		if !found {
			return nil, false, planner.missingInput("Route", name)
		}
		_, found = planner.identities[ids.KindService][spec.Target]
		if !found {
			return nil, false, planner.missingInput("Route target Service", spec.Target)
		}
		fingerprint, err := authoredUnitFingerprint("route", struct {
			Ordinal int
			Spec    core.RouteSpec
		}{index, spec})
		if err != nil {
			return nil, false, err
		}
		// This receipt acknowledges authored Route metadata, not serving traffic.
		// Its target id is pinned in the parent's immutable identities. Waiting
		// for the Service runtime would cycle: router -> Route -> Service ->
		// complete projection -> router. The router child owns the host effect.
		planner.addUnit(identity, fingerprint, nil, nil)
	}
	dnsComplete, err := planner.planDNSUnits(serviceBlocked)
	if err != nil {
		return nil, false, err
	}
	complete = complete && dnsComplete
	for name, identity := range planner.identities[ids.KindComponent] {
		spec, found := planner.input.desired.Input.Components[name]
		if !found {
			return nil, false, planner.missingInput("Component", name)
		}
		reads, after, componentComplete, err := planner.componentDependencies(name, spec)
		if err != nil {
			return nil, false, err
		}
		if !componentComplete {
			planner.block(identity)
			complete = false
			continue
		}
		forwardingClaims := planner.componentForwardingClaims(name)
		fingerprint, err := authoredUnitFingerprint("component", struct {
			Capability       string
			Spec             core.ComponentSpec
			Routes           []core.RouteSpec
			ForwardingClaims []blueprintunits.ResourceKey
		}{name, spec, planner.componentRoutes(name), forwardingClaims})
		if err != nil {
			return nil, false, err
		}
		planner.addUnit(identity, fingerprint, reads, after)
		if len(forwardingClaims) != 0 {
			target := planner.targetByID(identity.ID)
			unit := planner.units[target]
			unit.Writes = canonicalAuthoredUnitKeys(append(unit.Writes, forwardingClaims...))
			planner.units[target] = unit
		}
	}
	for name, identity := range planner.identities[ids.KindScript] {
		spec, found := planner.input.desired.Input.Scripts[name]
		if !found {
			return nil, false, planner.missingInput("Script", name)
		}
		reads, after, scriptComplete, err := planner.scriptDependencies(spec)
		if err != nil {
			return nil, false, err
		}
		if !scriptComplete {
			planner.block(identity)
			complete = false
			continue
		}
		fingerprint, err := authoredUnitFingerprint("script", struct {
			Key  string
			Spec core.ScriptSpec
		}{name, spec})
		if err != nil {
			return nil, false, err
		}
		planner.addUnit(identity, fingerprint, reads, after)
	}
	units := make([]blueprintunits.Unit, 0, len(planner.units))
	for _, unit := range planner.units {
		units = append(units, unit)
	}
	slices.SortFunc(units, func(left, right blueprintunits.Unit) int {
		return compareBlueprintUnitKeys(left.Target, right.Target)
	})
	return units, complete, nil
}
