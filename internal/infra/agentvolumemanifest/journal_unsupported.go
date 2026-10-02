//go:build !linux

package agentvolumemanifest

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Journal struct{}

func Open(context.Context, Config) (*Journal, error) {
	return nil, invalid("Volume manifest receiver requires Linux")
}
func (*Journal) Close() error { return nil }
func (*Journal) CurrentCredit(context.Context) (*agentpb.BackupVolumeManifestAckCredit, error) {
	return nil, invalid("Volume manifest receiver requires Linux")
}
func (*Journal) AcceptFrame(context.Context, *agentpb.BackupVolumeManifestTransfer) (
	*agentpb.BackupVolumeManifestAckCredit, error,
) {
	return nil, invalid("Volume manifest receiver requires Linux")
}
func (*Journal) ReadComplete(context.Context) ([]backupvolume.Entry, []backupvolume.ManifestEntryBytes, error) {
	return nil, nil, invalid("Volume manifest receiver requires Linux")
}
func (*Journal) Cleanup(context.Context) error {
	return invalid("Volume manifest receiver requires Linux")
}
func ResumeCleanup(context.Context, Config) error {
	return invalid("Volume manifest receiver requires Linux")
}
func Discard(context.Context, Config) error {
	return invalid("Volume manifest receiver requires Linux")
}
func ResumeDiscard(context.Context, Config) error {
	return invalid("Volume manifest receiver requires Linux")
}
func Inventory(context.Context, string) ([]Recovered, error) {
	return nil, invalid("Volume manifest receiver requires Linux")
}
func ReapEmptyRetired(context.Context, string) error {
	return invalid("Volume manifest receiver requires Linux")
}
