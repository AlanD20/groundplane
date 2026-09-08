package postgres16

import (
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/core"
)

func (a *adapter) CreationSpec(core.BackingAuthentication) adapters.CreationSpec {
	return adapters.CreationSpec{
		ServiceName:   "postgres",
		VolumeSlug:    "data",
		VolumeKey:     "data",
		MountPath:     "/var/lib/postgresql/data",
		HealthCommand: []string{"CMD-SHELL", "pg_isready -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\""},
		Expose:        []string{"5432"},
		HealthTCP:     "127.0.0.1:5432",
		Environment: []adapters.CreationEnvironment{
			{Name: "POSTGRES_DB", Literal: "postgres"},
			{Name: "POSTGRES_USER", Literal: "postgres"},
			{Name: "POSTGRES_PASSWORD", BootstrapKey: "password", Secret: true},
		},
	}
}
