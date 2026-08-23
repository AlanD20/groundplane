package api

// EntryEditRequest is the complete mutable desired state for an Entry.
// Identity, type, destination, storage class, and file ownership are immutable.
type EntryEditRequest struct {
	Source   EntrySource `json:"source"`
	Exposure []string    `json:"exposure"`
}
