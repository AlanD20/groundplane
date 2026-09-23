package networkreservations

import "maps"

func EqualComponentAddressRegistry(left, right ComponentAddressRegistry) bool {
	return (left.Reservations == nil) == (right.Reservations == nil) &&
		maps.Equal(left.Reservations, right.Reservations)
}
