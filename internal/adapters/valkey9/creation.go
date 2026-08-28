package valkey9

import "github.com/AlanD20/groundplane/internal/adapters"

func (a *adapter) CreationSpec() adapters.CreationSpec {
	return adapters.CreationSpec{
		ServiceName: "valkey",
		VolumeSlug:  "data",
		VolumeKey:   "data",
		MountPath:   "/data",
		Command: []string{
			"sh",
			"-ec",
			"exec valkey-server --appendonly yes --requirepass \"$VALKEY_PASSWORD\"",
		},
		HealthCommand: []string{"CMD-SHELL", "valkey-cli ping >/dev/null"},
		Expose:        []string{"6379"},
		HealthTCP:     "127.0.0.1:6379",
		Environment: []adapters.CreationEnvironment{
			{Name: "VALKEY_PASSWORD", BootstrapKey: "password", Secret: true},
			{Name: "REDISCLI_AUTH", BootstrapKey: "password", Secret: true},
		},
	}
}
