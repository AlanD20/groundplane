package app

import (
	"context"
	"errors"
	"io"

	"github.com/AlanD20/groundplane/internal/agent/backupruntime"

	"github.com/AlanD20/groundplane/internal/agent/attachruntime"
	"github.com/AlanD20/groundplane/internal/agent/backingruntime"
	"github.com/AlanD20/groundplane/internal/agent/componentruntime"
	composeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	"github.com/AlanD20/groundplane/internal/agent/entryruntime"
	directoryruntime "github.com/AlanD20/groundplane/internal/agent/environmentdirectory"
	"github.com/AlanD20/groundplane/internal/agent/environmentruntime"
	"github.com/AlanD20/groundplane/internal/agent/hierarchyruntime"
	"github.com/AlanD20/groundplane/internal/agent/lifecycleruntime"
	filematerialization "github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/agent/networkruntime"
	"github.com/AlanD20/groundplane/internal/agent/releaseruntime"
	"github.com/AlanD20/groundplane/internal/agent/removalruntime"
	"github.com/AlanD20/groundplane/internal/agent/resolverruntime"
	scriptruntime "github.com/AlanD20/groundplane/internal/agent/scriptruntime"
	"github.com/AlanD20/groundplane/internal/agent/volumeruntime"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"

	"github.com/AlanD20/groundplane/internal/infra/docker/containerlogs"
	"github.com/AlanD20/groundplane/internal/infra/docker/dnsresolverobserver"
	"github.com/AlanD20/groundplane/internal/infra/docker/hostresolutionhelpercontainer"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelpercontainer"
	"github.com/AlanD20/groundplane/pkg/errs"
	mobyclient "github.com/moby/moby/client"
)

type ownedComposeHelper interface {
	entryruntime.Helper
	componentruntime.Helper
	networkruntime.Helper
	volumeruntime.Helper
	backingruntime.Helper
	resolverruntime.Helper
	lifecycleruntime.Helper
	backupruntime.BackupConsumerHelper
	removalruntime.Helper
	attachruntime.ConsumerHelper
	hierarchyruntime.ConsumerHelper
	environmentruntime.Host
	hierarchyruntime.EnvironmentHelper
	composeruntime.Helper
	releaseruntime.MutationHelper
	releaseruntime.RecoveryHostHelper
	io.Closer
}

type ownedComposeObserver interface {
	composeruntime.Observer
	releaseruntime.Observer
	io.Closer
}

type ownedEnvironmentDirectoryHelper interface {
	directoryruntime.Helper
	io.Closer
}

type ownedMaterializationHelper interface {
	filematerialization.Helper
	io.Closer
}

type agentRuntimeResources struct {
	compose                *agentComposeResources
	environmentDirectories ownedEnvironmentDirectoryHelper
	materializer           ownedMaterializationHelper
	managedConfigs         *managedconfighelpercontainer.Executor
	dnsResolverObserver    *dnsresolverobserver.Executor
	hostResolution         *hostresolutionhelpercontainer.Executor
	logs                   *containerlogs.Reader
	scripts                *scriptruntime.Runtime
	dockerReads            *mobyclient.Client
	workJournal            *agentworkjournal.Journal
	ownership              *agentownership.Lease
}

type agentComposeResources struct {
	helper   ownedComposeHelper
	observer ownedComposeObserver
}

func (resources *agentRuntimeResources) Close() error {
	if resources == nil {
		return nil
	}
	var logErr, scriptErr, hostResolutionErr, managedConfigErr, dnsObserverErr, materializerErr, directoryErr, composeErr error
	var imageErr, workJournalErr error
	if resources.workJournal != nil {
		workJournalErr = resources.workJournal.Close()
	}
	if resources.dockerReads != nil {
		imageErr = resources.dockerReads.Close()
	}
	if resources.logs != nil {
		logErr = resources.logs.Close()
	}
	if resources.scripts != nil {
		scriptErr = resources.scripts.Close()
	}
	if resources.materializer != nil {
		materializerErr = resources.materializer.Close()
	}
	if resources.managedConfigs != nil {
		managedConfigErr = resources.managedConfigs.Close()
	}
	if resources.dnsResolverObserver != nil {
		dnsObserverErr = resources.dnsResolverObserver.Close()
	}
	if resources.hostResolution != nil {
		hostResolutionErr = resources.hostResolution.Close()
	}
	if resources.environmentDirectories != nil {
		directoryErr = resources.environmentDirectories.Close()
	}
	if resources.compose != nil {
		composeErr = resources.compose.Close()
	}
	// The process lock outlives every execution resource and worker.
	ownershipErr := resources.ownership.Close()
	if joined := errors.Join(
		ownershipErr,
		workJournalErr,
		imageErr,
		logErr,
		scriptErr,
		hostResolutionErr,
		managedConfigErr,
		dnsObserverErr,
		materializerErr,
		directoryErr,
		composeErr,
	); joined != nil {
		return errs.Wrap(errs.KindInternal, joined)
	}
	return nil
}

func newAgentComposeResources(
	image string,
	newHelper func(string) (ownedComposeHelper, error),
	newObserver func() (ownedComposeObserver, error),
) (*agentComposeResources, error) {
	if image == "" {
		return nil, errs.New(errs.KindValidationFailed, "agent: managed Agent image is required")
	}
	if newHelper == nil || newObserver == nil {
		return nil, errs.New(errs.KindInternal, "agent: Compose helper factories are required")
	}
	helper, err := newHelper(image)
	if err != nil {
		return nil, err
	}
	observer, err := newObserver()
	if err != nil {
		return nil, preferAgentComposeCleanup(err, helper.Close())
	}
	return &agentComposeResources{helper: helper, observer: observer}, nil
}

func (resources *agentComposeResources) Close() error {
	if resources == nil {
		return nil
	}
	var observerErr, helperErr error
	if resources.observer != nil {
		observerErr = resources.observer.Close()
	}
	if resources.helper != nil {
		helperErr = resources.helper.Close()
	}
	if joined := errors.Join(observerErr, helperErr); joined != nil {
		return errs.Wrap(errs.KindInternal, joined)
	}
	return nil
}

func preferAgentComposeCleanup(operationErr, cleanupErr error) error {
	if cleanupErr == nil {
		return operationErr
	}
	if operationErr == nil || errors.Is(operationErr, context.Canceled) ||
		errors.Is(operationErr, context.DeadlineExceeded) {
		return cleanupErr
	}
	return errs.Wrap(errs.KindInternal, errors.Join(operationErr, cleanupErr))
}
