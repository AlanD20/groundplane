package attachments

// Reader owns Attach lookup, reference impact and encrypted-fact snapshots.
type Reader struct{ store snapshotReadStore }

// NewReader binds Attach reads to an already configured persistence store.
func NewReader(store snapshotReadStore) *Reader { return &Reader{store: store} }
