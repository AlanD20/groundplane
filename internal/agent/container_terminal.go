package agent

import (
	"github.com/AlanD20/groundplane/internal/agent/containerterminal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (c *Client) SetContainerTerminalOpener(opener containerterminal.Opener) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || opener == nil {
		return errs.New(errs.KindStateConflict, "container terminal must be configured before Agent startup")
	}
	c.containerTerminals = opener
	return nil
}
