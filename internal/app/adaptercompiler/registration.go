package adaptercompiler

import (
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/adapters/custom"
	"github.com/AlanD20/groundplane/internal/adapters/postgres16"
	"github.com/AlanD20/groundplane/internal/adapters/valkey9"
)

// Register installs the closed built-in catalog at process composition.
func Register() {
	if _, registered := adapters.Get("postgres:16"); !registered {
		postgres16.Register()
	}
	if _, registered := adapters.Get("valkey:9"); !registered {
		valkey9.Register()
	}
	if _, registered := adapters.Get("custom"); !registered {
		custom.Register()
	}
}
