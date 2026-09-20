package taskplan

import "github.com/AlanD20/groundplane/proto/agentpb"

func ClearBackingHookProcedure(procedure *agentpb.BackingHookProcedure) {
	if procedure == nil {
		return
	}
	for _, value := range append(procedure.Inputs, procedure.Facts...) {
		if value != nil {
			clear(value.Value)
			value.Value = nil
		}
	}
}
