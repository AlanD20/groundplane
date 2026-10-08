//go:build !linux

package agentpostgresjournal

import "context"

func Mark(context.Context, IDs) error {
	return invalid("database execution journal requires Linux")
}

func Inventory(context.Context) ([]IDs, error) {
	return nil, invalid("database execution journal requires Linux")
}

func Remove(context.Context, IDs) error {
	return invalid("database execution journal requires Linux")
}
