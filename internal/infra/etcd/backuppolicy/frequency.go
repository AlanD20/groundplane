package backuppolicy

import "github.com/AlanD20/groundplane/internal/common/backupschedule"

// MaximumBackupPolicySources is the largest selection whose worst-case
// protected replacement fits the fixed 96-operation etcd transaction budget.
// The worst case is an enabled Connector move that creates era 1 and selects
// only Attach/Volume sources: 24 fixed operations plus 6 per source.
const MaximumBackupPolicySources = 12

func ValidateFrequency(frequency string) error {
	_, err := backupschedule.Parse(frequency)
	return err
}
