package agent

import (
	"github.com/AlanD20/groundplane/internal/agent/backingadapter"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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
