package adapters

import "github.com/AlanD20/groundplane/internal/common/backinghook"

// HookInput encodes the built-in resolved fields into the same named inputs
// exposed as GP_INPUT_* to shell hooks. The caller retains all value ownership.
func (input Input) HookInput() backinghook.Input {
	values := append([]backinghook.Value(nil), input.Values...)
	for _, value := range []backinghook.Value{
		{Key: "AUTHENTICATION", Value: []byte(input.Authentication)},
		{Key: "DATABASE", Value: []byte(input.Database)},
		{Key: "ROLE", Value: []byte(input.Role)},
		{Key: "PASSWORD", Value: input.Password},
		{Key: "GRANT_ON", Value: []byte(input.GrantOn)},
		{Key: "HOST", Value: []byte(input.Host)},
		{Key: "PORT", Value: []byte(input.Port)},
	} {
		if len(value.Value) != 0 {
			values = append(values, value)
		}
	}
	return backinghook.Input{Context: input.Context, Values: values, Facts: input.Facts}
}
