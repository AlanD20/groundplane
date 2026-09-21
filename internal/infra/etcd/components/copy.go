package components

import "github.com/AlanD20/groundplane/internal/core"

func CloneRecord(record Record) Record {
	clone := record
	clone.Desired.Config = core.CloneComponentConfig(record.Desired.Config)
	clone.Runtime.GeneratedServices = append([]string(nil), record.Runtime.GeneratedServices...)
	return clone
}
