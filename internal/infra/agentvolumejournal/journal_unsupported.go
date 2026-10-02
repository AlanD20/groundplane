//go:build !linux

package agentvolumejournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
)

type Journal struct{}

func Open(context.Context, Config) (*Journal, error) {
	return nil, invalid("Volume mutation journal requires Linux")
}
func (*Journal) Close() error { return nil }
func (*Journal) ReadState(context.Context) (State, error) {
	return State{}, invalid("Volume mutation journal requires Linux")
}
func (*Journal) Intent(context.Context, backupvolumefs.Mutation) error {
	return invalid("Volume mutation journal requires Linux")
}
func (*Journal) Completed(context.Context, backupvolumefs.Mutation) error {
	return invalid("Volume mutation journal requires Linux")
}
func (*Journal) Cleanup(context.Context) error {
	return invalid("Volume mutation journal requires Linux")
}
func ResumeCleanup(context.Context, Config) error {
	return invalid("Volume mutation journal requires Linux")
}
func Discard(context.Context, Config) error {
	return invalid("Volume mutation journal requires Linux")
}
func ResumeDiscard(context.Context, Config) error {
	return invalid("Volume mutation journal requires Linux")
}
func Inventory(context.Context, string) ([]Recovered, error) {
	return nil, invalid("Volume mutation journal requires Linux")
}
func ReapEmptyRetired(context.Context, string) error {
	return invalid("Volume mutation journal requires Linux")
}
func Inspect(context.Context, string, string) (Recovered, bool, error) {
	return Recovered{}, false, invalid("Volume mutation journal requires Linux")
}
