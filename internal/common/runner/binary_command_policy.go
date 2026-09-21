package runner

import (
	"bytes"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateBinarySink(sink BinarySink) error {
	if sink.Writer == nil || sink.Interrupt == nil {
		return errs.New(errs.KindValidationFailed, "runner: binary sink requires writer and interrupt")
	}
	return nil
}

func (r *OSRunner) validateBinaryCommand(opts RunCmdOpts) error {
	policy, ok := r.binaryCommands[opts.Name]
	if !ok || policy.validate == nil || !policy.validate(opts) {
		return errs.New(errs.KindValidationFailed, "runner: binary command is not allowlisted")
	}
	return nil
}

func validDockerBinaryCommand(opts RunCmdOpts) bool {
	if opts.Dir != "" || len(opts.Env) != 0 || opts.ReplaceEnv || opts.Stdin != nil ||
		len(opts.StderrRedactions) != 0 {
		return false
	}
	args := opts.Args
	if len(args) != 16 || args[0] != "container" || args[1] != "exec" || args[2] != "-i" ||
		args[3] != "--user" || args[4] != "postgres" || args[6] != "pg_dump" {
		return false
	}
	const databasePrefix = "--dbname="
	const rolePrefix = "--role="
	if !validContainerID(args[5]) || args[7] != "--format=custom" || args[8] != "--compress=0" ||
		args[9] != "--no-owner" || args[10] != "--no-acl" ||
		args[11] != "--host=/var/run/postgresql" || args[12] != "--username=postgres" ||
		args[13] != "--no-password" || !bytes.HasPrefix([]byte(args[14]), []byte(rolePrefix)) ||
		!bytes.HasPrefix([]byte(args[15]), []byte(databasePrefix)) {
		return false
	}
	command := PostgresDumpCommand{
		ContainerID: args[5],
		Database:    args[15][len(databasePrefix):],
		Role:        args[14][len(rolePrefix):],
		Timeout:     opts.Timeout,
	}
	canonical, err := command.runCmdOpts()
	return err == nil && equalStrings(args, canonical.Args)
}

func validContainerID(value string) bool {
	if len(value) < 12 || len(value) > 64 {
		return false
	}
	for _, character := range []byte(value) {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
