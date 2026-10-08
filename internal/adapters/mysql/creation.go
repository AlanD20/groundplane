package mysql

import (
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/core"
)

func (a *adapter) CreationSpec(_ string, _ core.BackingAuthentication) adapters.CreationSpec {
	return adapters.CreationSpec{
		ServiceName:   "mysql",
		VolumeSlug:    "data",
		VolumeKey:     "data",
		MountPath:     "/var/lib/mysql",
		HealthCommand: []string{"CMD", "mysqladmin", "ping", "--protocol=socket", "--silent"},
		Expose:        []string{"3306"},
		HealthTCP:     "127.0.0.1:3306",
		Environment: []adapters.CreationEnvironment{
			{Name: "MYSQL_ROOT_PASSWORD", BootstrapKey: "password", Secret: true},
		},
	}
}
