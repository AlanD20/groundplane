package api

// EntryEditRequest is the complete mutable desired state for an Entry.
// Identity, type, destination, confidentiality class, and file ownership are immutable.
type EntryEditRequest struct {
	Source     EntrySource `json:"source"`
	Exposure   []string    `json:"exposure"`
	Encryption string      `json:"encryption,omitempty" enum:"age,none"`
	After      []string    `json:"after,omitempty" maxItems:"16" uniqueItems:"true"`
}
