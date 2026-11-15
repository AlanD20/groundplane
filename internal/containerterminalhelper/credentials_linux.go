// Package containerterminalhelper owns the one supervised process tree inside
// a selected container. Its framed pipes are private to the authenticated Agent.
package containerterminalhelper

import (
	"os"
	"os/user"
	"strconv"
	"strings"
	"syscall"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func credentials(value string) (*syscall.Credential, string, error) {
	parts := strings.Split(value, ":")
	if len(parts) > 2 || parts[0] == "" {
		return nil, "", errs.New(errs.KindValidationFailed, "container user is invalid")
	}
	account, lookupErr := user.Lookup(parts[0])
	uid, numericErr := strconv.ParseUint(parts[0], 10, 32)
	if numericErr == nil {
		account, lookupErr = user.LookupId(parts[0])
	} else if lookupErr == nil {
		uid, numericErr = strconv.ParseUint(account.Uid, 10, 32)
	}
	if numericErr != nil {
		return nil, "", errs.New(errs.KindValidationFailed, "container user does not exist")
	}
	gid, home := uint64(0), "/"
	var groups []uint32
	if lookupErr == nil {
		gid, _ = strconv.ParseUint(account.Gid, 10, 32)
		home = account.HomeDir
		if memberships, err := account.GroupIds(); err == nil {
			for _, membership := range memberships {
				if group, err := strconv.ParseUint(membership, 10, 32); err == nil {
					groups = append(groups, uint32(group))
				}
			}
		}
	}
	if len(parts) == 2 {
		var err error
		gid, err = strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			group, lookupErr := user.LookupGroup(parts[1])
			if lookupErr != nil {
				return nil, "", errs.New(errs.KindValidationFailed, "container group does not exist")
			}
			gid, err = strconv.ParseUint(group.Gid, 10, 32)
			if err != nil {
				return nil, "", errs.New(errs.KindValidationFailed, "container group is invalid")
			}
		}
	}
	return &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid), Groups: groups}, home, nil
}

func commandEnvironment(home string) []string {
	result := make([]string, 0, len(os.Environ())+2)
	for _, value := range os.Environ() {
		if strings.HasPrefix(value, "TERM=") || strings.HasPrefix(value, "HOME=") {
			continue
		}
		result = append(result, value)
	}
	return append(result, "TERM=xterm-256color", "HOME="+home)
}
