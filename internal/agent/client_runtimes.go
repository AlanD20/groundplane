package agent

import (
	"github.com/AlanD20/groundplane/internal/agent/backingadapter"
	"github.com/AlanD20/groundplane/internal/agent/hierarchyruntime"
	"github.com/AlanD20/groundplane/internal/agent/networkruntime"
	"github.com/AlanD20/groundplane/internal/agent/releaseruntime"
	"github.com/AlanD20/groundplane/internal/agent/scriptruntime"
	"github.com/AlanD20/groundplane/internal/agent/volumeruntime"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SetProcessOwnership borrows the process-lifetime lease. Application shutdown
// releases it only after Run has joined every session worker.
func (c *Client) SetProcessOwnership(ownership *agentownership.Lease) error {
	if c == nil {
		return errs.New(errs.KindInternal, "Agent client is required")
	}
	if err := ownership.Check(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.processOwnership != nil {
		return errs.New(errs.KindStateConflict, "Agent process ownership cannot change after configuration")
	}
	c.processOwnership = ownership
	return nil
}

func (c *Client) SetScriptExecutor(executor *scriptruntime.Executor) error {
	if c == nil || executor == nil {
		return errs.New(errs.KindValidationFailed, "Agent Script executor is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.scriptExecutor != nil {
		return errs.New(errs.KindStateConflict, "Agent Script executor cannot change after configuration")
	}
	c.scriptExecutor = executor
	return nil
}

func (c *Client) SetAttachRemovalExecutor(executor *hierarchyruntime.AttachExecutor) error {
	if c == nil || executor == nil {
		return errs.New(errs.KindValidationFailed, "Agent Attach removal executor is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.attachRemovalExecutor != nil {
		return errs.New(errs.KindStateConflict, "Agent Attach removal executor cannot change after configuration")
	}
	c.attachRemovalExecutor = executor
	return nil
}

func (c *Client) SetNetworkExecutor(executor *networkruntime.Executor) error {
	if c == nil || executor == nil {
		return errs.New(errs.KindValidationFailed, "Agent Network executor is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.networkExecutor != nil {
		return errs.New(errs.KindStateConflict, "Agent Network executor cannot change after configuration")
	}
	c.networkExecutor = executor
	return nil
}

func (c *Client) SetVolumeExecutor(executor *volumeruntime.Executor) error {
	if c == nil || executor == nil {
		return errs.New(errs.KindValidationFailed, "Agent Volume executor is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.volumeExecutor != nil {
		return errs.New(errs.KindStateConflict, "Agent Volume executor cannot change after configuration")
	}
	c.volumeExecutor = executor
	return nil
}

func (c *Client) SetHierarchyExecutor(executor *hierarchyruntime.Executor) error {
	if c == nil || executor == nil {
		return errs.New(errs.KindValidationFailed, "Agent hierarchy executor is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.hierarchyExecutor != nil {
		return errs.New(errs.KindStateConflict, "Agent hierarchy executor cannot change after configuration")
	}
	c.hierarchyExecutor = executor
	return nil
}

func (c *Client) SetReleaseExecutor(executor *releaseruntime.Executor) error {
	if c == nil || executor == nil {
		return errs.New(errs.KindValidationFailed, "Agent Release executor is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.releaseExecutor != nil {
		return errs.New(errs.KindStateConflict, "Agent Release executor cannot change after configuration")
	}
	c.releaseExecutor = executor
	return nil
}

func (c *Client) SetReleaseRecoveryExecutor(executor *releaseruntime.RecoveryExecutor) error {
	if c == nil || executor == nil {
		return errs.New(errs.KindValidationFailed, "Agent Release recovery executor is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.releaseRecoveryExecutor != nil {
		return errs.New(errs.KindStateConflict, "Agent Release recovery executor cannot change after configuration")
	}
	c.releaseRecoveryExecutor = executor
	return nil
}

func (c *Client) SetComponentActionRuntime(runtime ComponentActionRuntime) error {
	if c == nil || runtime == nil {
		return errs.New(errs.KindValidationFailed, "agent: Component action runtime is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.pool != nil {
		return errs.New(errs.KindStateConflict, "agent: Component action runtime cannot change after start")
	}
	c.componentActions = runtime
	return nil
}

func (c *Client) SetHostResolutionRuntime(runtime HostResolutionRuntime) error {
	if c == nil || runtime == nil {
		return errs.New(errs.KindValidationFailed, "agent: host resolution runtime is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.pool != nil {
		return errs.New(errs.KindStateConflict, "agent: host resolution runtime cannot change after start")
	}
	c.hostResolution = runtime
	return nil
}

func (c *Client) SetScriptRuntime(runtime ScriptRuntime) error {
	if c == nil || runtime == nil {
		return errs.New(errs.KindValidationFailed, "agent: Script runtime is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.pool != nil {
		return errs.New(errs.KindStateConflict, "agent: Script runtime cannot change after start")
	}
	c.scriptRuntime = runtime
	return nil
}

func (c *Client) SetAdapterCompiler(compiler backingadapter.Compiler) error {
	if c == nil || compiler == nil {
		return errs.New(errs.KindValidationFailed, "agent: adapter compiler is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.pool != nil {
		return errs.New(errs.KindStateConflict, "agent: adapter compiler cannot change after start")
	}
	c.adapterCompiler = compiler
	return nil
}
