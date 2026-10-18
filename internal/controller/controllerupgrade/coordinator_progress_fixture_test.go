package controllerupgrade

import "context"

func (h *coordinatorHarness) progress(ctx context.Context) *workObserver {
	return &workObserver{ctx: ctx, terminalOnly: true}
}
