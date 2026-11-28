package releaseoperation

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/remotefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/hostplacement"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/core/serviceports"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/remoteimages"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RemoteEnvironmentExpectation selects a previously prepared Environment on
// one remote host. The attestation precedes deployment and does not claim that
// any application Release is serving it.
type RemoteEnvironmentExpectation struct {
	Executor        workexecution.Identity
	Scope           workexecution.Scope
	ServiceID       string
	DesiredRevision int64
	EpochRevision   int64
	ReadRevision    int64
}

type RemoteEnvironmentWitness struct {
	ProxyImage domain.ProxyImage
	Conditions []keyvalue.Condition
}

// RemoteEnvironmentAttestor verifies host-local infrastructure at the fixed
// desired revision: private edge reachability, exact proxy asset, isolated
// network and runtime storage, and absence of backing connections. The checks
// must return durable comparisons owned by the corresponding lifecycle paths.
type RemoteEnvironmentAttestor interface {
	CaptureRemoteEnvironment(context.Context, RemoteEnvironmentExpectation) (RemoteEnvironmentWitness, error)
}

type remoteDeploymentPreparation struct {
	hosts        *hostplacement.Resolver
	environments RemoteEnvironmentAttestor
	store        hostbindings.Reader
}

// ConfigureRemoteDeployment is called during composition, before any requests
// can reach the Service. Remote placement remains unavailable unless all three
// authorities have been wired.
func (service *Service) ConfigureRemoteDeployment(
	store hostbindings.Reader,
	environments RemoteEnvironmentAttestor,
) error {
	if service == nil || environments == nil {
		return remotePlacementUnavailable()
	}
	hosts, err := hostplacement.New(store)
	if err != nil {
		return err
	}
	service.remotePlacement = &remoteDeploymentPreparation{hosts: hosts, environments: environments, store: store}
	return nil
}

func (service *Service) prepareRemoteServiceWork(ctx context.Context, scope releasequeries.ReleasePlanningScope,
	candidate releaseCandidateInput, task workexecution.Task, hostID string,
) (taskdependencies.Prepared, error) {
	remote := service.remotePlacement
	if remote == nil || task.Action != workexecution.ActionDeploy || task.Scope.TenantID == "" ||
		candidate.strategy != domain.StrategyRecreate || candidate.selection.Requested == nil {
		return taskdependencies.Prepared{}, remotePlacementUnavailable()
	}
	requested := candidate.selection.Requested
	imageReference, err := remotefetch.Reference(requested.Reference)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if requested.Replicas != 1 {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindValidationFailed,
			"remote deployment requires exactly one replica",
		)
	}
	selected, err := remote.hosts.ResolveRemote(ctx, hostID, scope.ReadRevision,
		workexecution.Release, workexecution.Network)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	prepared, err := remote.environments.CaptureRemoteEnvironment(ctx, RemoteEnvironmentExpectation{
		Executor: selected.Identity, Scope: task.Scope, ServiceID: task.TargetID,
		DesiredRevision: candidate.planning.Service.Revision, EpochRevision: scope.EnvironmentEpochRevision,
		ReadRevision: scope.ReadRevision,
	})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	proxy := prepared.ProxyImage.Platform
	if prepared.ProxyImage.Validate() != nil || proxy.OS != selected.Platform.OS ||
		proxy.Architecture != selected.Platform.Architecture || proxy.Variant != selected.Platform.Variant &&
		!(proxy.Architecture == "arm64" && (proxy.Variant == "" || selected.Platform.Variant == "")) ||
		len(
			prepared.Conditions,
		) == 0 || slices.ContainsFunc(prepared.Conditions, func(condition keyvalue.Condition) bool {
		return condition.Key == "" || condition.ModRevision < 0 || condition.ModRevision > scope.ReadRevision
	}) {
		return taskdependencies.Prepared{}, remotePlacementUnavailable()
	}
	image, err := remoteimages.ReadWitness(ctx, remote.store, hostID, imageReference, scope.ReadRevision)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if image.Revision <= 0 || image.Record.Executor.AgentID != selected.Identity.AgentID ||
		image.Record.Executor.Generation > selected.Identity.Generation {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"selected host has no matching successful image Fetch",
		)
	}
	conditions := append(selected.Conditions, prepared.Conditions...)
	conditions = append(
		conditions,
		keyvalue.Condition{Key: remoteimages.WitnessKey(hostID, imageReference), ModRevision: image.Revision},
	)
	candidate.remoteProxyImage = &prepared.ProxyImage
	return service.prepareServiceWorkForHost(ctx, scope, candidate, task, &remoteServiceSelection{
		host: selected, localImageID: image.Record.LocalImageID, conditions: conditions,
	})
}

func validateRemoteDeploymentProjection(scope releasequeries.ReleasePlanningScope,
	projection environmentprojection.EnvironmentComposeProjection, candidate releaseCandidateInput,
) error {
	desired := candidate.planning.Service.Record.Desired
	if projection.EnvironmentID != scope.Environment.Record.ID || projection.BackingRuntime != nil ||
		len(projection.RuntimeFiles) != 0 || len(projection.Volumes) != 0 || len(projection.VolumeMounts) != 0 ||
		projection.Backup != nil || len(projection.Authored) != 0 || len(projection.DesiredServices) != 1 ||
		projection.DesiredServices[0].Desired.ID != desired.ID || len(projection.DesiredRoutes) == 0 ||
		len(desired.Mounts) != 0 || len(desired.DependsOn) != 0 || desired.Hooks != nil ||
		desired.Adapter != "" || candidate.planning.Service.Record.BackingNetworkID != "" || len(desired.ProxyExposures()) == 0 {
		return unsupportedRemoteEnvironment()
	}
	for _, entry := range projection.Entries {
		if entry.Entry.Kind != core.EntryKindEnv || entry.Entry.Source.Kind != core.SourceLiteral {
			return unsupportedRemoteEnvironment()
		}
	}
	for _, entry := range desired.Environment {
		if entry.Kind != core.EntryKindEnv || entry.Source.Kind != core.SourceLiteral {
			return unsupportedRemoteEnvironment()
		}
	}
	ports, err := domain.ProxyPorts(desired.ProxyExposures())
	if err != nil {
		return err
	}
	for _, route := range projection.DesiredRoutes {
		if route.Desired.TargetServiceID != desired.ID || route.Desired.Exposure != "public" ||
			!slices.Contains(ports, domain.ProxyPort{Port: route.Desired.TargetPort, Protocol: serviceports.TCP}) {
			return unsupportedRemoteEnvironment()
		}
	}
	enabledProxy := false
	for _, component := range projection.Components {
		if component.Desired.Enabled {
			if enabledProxy || !component.Desired.Kind.IsEnvironmentProxy() ||
				component.Desired.OwnerID != projection.EnvironmentID {
				return unsupportedRemoteEnvironment()
			}
			enabledProxy = true
		}
	}
	if !enabledProxy {
		return unsupportedRemoteEnvironment()
	}
	return nil
}

func unsupportedRemoteEnvironment() error {
	return errs.New(
		errs.KindValidationFailed,
		"remote deployment requires an isolated stateless HTTP environment with one Service, a prepared router, and no shared or persistent dependencies",
	)
}

func remotePlacementUnavailable() error {
	return errs.New(errs.KindStateConflict, "remote service placement is unavailable for this deployment")
}
