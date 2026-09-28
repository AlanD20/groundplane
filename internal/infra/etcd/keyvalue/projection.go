package keyvalue

const (
	DefaultPageLimit = 50
	MaximumPageLimit = 200
)

type Versioned[T any] struct {
	Record       T
	Revision     int64
	ReadRevision int64
}

type PageRequest struct {
	Limit      int
	Cursor     string
	Revision   int64
	Descending bool
}

// NewestFirst orders time-encoded resource IDs descending.
func (request PageRequest) NewestFirst() PageRequest {
	request.Descending = true
	return request
}

type Page[T any] struct {
	Items      []Versioned[T]
	NextCursor string
	Revision   int64
}
