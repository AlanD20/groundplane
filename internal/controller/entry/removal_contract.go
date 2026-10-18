package entry

type RemoveRequest struct {
	EntryID        string
	IdempotencyKey string
}

// RemovalOutcome is the accepted native action identity. HTTP serialization
// belongs to the API adapter, not to Entry execution or persistence.
type RemovalOutcome struct {
	TaskID string
}
