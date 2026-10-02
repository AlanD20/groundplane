package agentconfigjournal

import (
	"context"
	"os"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"golang.org/x/sys/unix"
)

// The stage reservation covers the journal too, so both must consume space
// from the same filesystem. A separately mounted journal is not admitted.
func validateJournalFilesystem(ctx context.Context, directory *os.File) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stageFD, err := unix.Open(backupstage.AgentRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fileError(err)
	}
	defer unix.Close(stageFD)
	var stageStat, journalStat unix.Stat_t
	var filesystem unix.Statfs_t
	if err := unix.Fstat(stageFD, &stageStat); err != nil {
		return fileError(err)
	}
	if err := unix.Fstat(int(directory.Fd()), &journalStat); err != nil {
		return fileError(err)
	}
	if err := unix.Fstatfs(int(directory.Fd()), &filesystem); err != nil {
		return fileError(err)
	}
	if stageStat.Dev != journalStat.Dev || filesystem.Bsize <= 0 ||
		uint64(filesystem.Bsize) > backupconfigtransfer.JournalAllocationUnit {
		return conflict("Config journal filesystem differs from its staging capacity reservation")
	}
	return nil
}
