package postgres16protocol

// The container supervisor needs this closed root capability set to launch
// and stop its fixed unprivileged client. The client gate drops all of them
// before executing pg_dump, pg_restore or psql.
func ManagedRootCapabilities() []string {
	return []string{"CHOWN", "DAC_OVERRIDE", "FOWNER", "KILL", "SETGID", "SETUID", "SETPCAP", "SYS_PTRACE"}
}

func ManagedContainerSecurityOptions() []string {
	return []string{"no-new-privileges", "seccomp=unconfined"}
}

// ManagedLaunchProfileSHA256 binds the fixed release profile independently of
// per-execution nonces, process identities and kernel capability enumeration.
// Client files and the architecture-specific BPF program have separate hashes.
func ManagedLaunchProfileSHA256() Digest {
	body := appendUint32(nil, ProtocolVersion)
	body = appendUint32(body, AdapterContractVersion)
	body = appendUint32(body, PostgreSQLMajor)
	body = appendUint32(body, PostgreSQLUID)
	body = appendUint32(body, PostgreSQLGID)
	body = appendUint32(body, HelperMode)
	body = appendUint32(body, ClientGateMode)
	body = appendUint32(body, StateDirectoryMode)
	body = appendUint32(body, StateFileMode)
	body = appendUint32(body, ClientNoFileLimit)
	body = appendUint64(body, confinementRootCapabilityMask)
	for _, value := range []string{HelperPath, ClientGatePath, StateDirectoryPath, PGDumpPath, PGRestorePath, PSQLPath} {
		body = append(body, value...)
		body = append(body, 0)
	}
	for _, value := range exactEnvironment {
		body = append(body, value...)
		body = append(body, 0)
	}
	return domainSeparatedDigest("groundplane.postgres16.managed-launch-profile.v1", body)
}
