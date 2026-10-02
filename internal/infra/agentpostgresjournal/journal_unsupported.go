//go:build !linux

package agentpostgresjournal

import "context"

func Mark(context.Context, IDs) error {
	return invalid("PostgreSQL execution journal requires Linux")
}

func Inventory(context.Context) ([]IDs, error) {
	return nil, invalid("PostgreSQL execution journal requires Linux")
}

func Remove(context.Context, IDs) error {
	return invalid("PostgreSQL execution journal requires Linux")
}
