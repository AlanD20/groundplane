package ids

import (
	"crypto/rand"
	"encoding/hex"
	"io"
)

// ImageCorrelationCounter belongs to one authenticated connection. Its owner
// serializes Next; exhaustion never wraps and never affects Task traffic.
type ImageCorrelationCounter struct {
	next      [16]byte
	available bool
}

func NewImageCorrelationCounter() ImageCorrelationCounter {
	return imageCorrelationFromReader(rand.Reader)
}

func imageCorrelationFromReader(reader io.Reader) ImageCorrelationCounter {
	var counter ImageCorrelationCounter
	n, err := reader.Read(counter.next[:])
	counter.available = err == nil && n == len(counter.next) && counter.next != [16]byte{}
	return counter
}

func (counter *ImageCorrelationCounter) Next() (string, bool) {
	if !counter.available {
		return "", false
	}
	value := hex.EncodeToString(counter.next[:])
	for index := len(counter.next) - 1; index >= 0; index-- {
		counter.next[index]++
		if counter.next[index] != 0 {
			return value, true
		}
	}
	counter.available = false
	return value, true
}
