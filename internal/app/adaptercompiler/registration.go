package adaptercompiler

import (
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/adapters/custom"
	"github.com/AlanD20/groundplane/internal/adapters/mysql"
	"github.com/AlanD20/groundplane/internal/adapters/postgres"
	"github.com/AlanD20/groundplane/internal/adapters/valkey"
)

// Register installs the closed built-in catalog at process composition.
func Register() {
	if _, registered := adapters.Get("mysql"); !registered {
		mysql.Register()
	}
	if _, registered := adapters.Get("postgres"); !registered {
		postgres.Register()
	}
	if _, registered := adapters.Get("valkey"); !registered {
		valkey.Register()
	}
	if _, registered := adapters.Get("custom"); !registered {
		custom.Register()
	}
}
