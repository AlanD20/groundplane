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
	Limit    int
	Cursor   string
	Revision int64
}

type Page[T any] struct {
	Items      []Versioned[T]
	NextCursor string
	Revision   int64
}
