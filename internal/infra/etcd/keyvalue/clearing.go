package keyvalue

import ()

func ClearValues(values []*KeyValue) {
	for _, value := range values {
		if value != nil {
			clear(value.Value)
			value.Value = nil
		}
	}
}
