package backupvolume

import (
	"bytes"
	"strconv"
	"unicode/utf8"
)

type headerPlan struct {
	hasPAX       bool
	paxHeader    [TarBlockBytes]byte
	paxPayload   []byte
	memberHeader [TarBlockBytes]byte
}

type paxValues struct {
	path    []byte
	uid     uint32
	gid     uint32
	size    uint64
	hasPath bool
	hasUID  bool
	hasGID  bool
	hasSize bool
}

func planHeader(entry Entry, zeroOrdinal uint64) (headerPlan, error) {
	name, prefix, pathFits := splitUSTARPath(entry.Path)
	pathOverride := !pathFits
	uidOverride := uint64(entry.UID) > maximumSmallOctal
	gidOverride := uint64(entry.GID) > maximumSmallOctal
	sizeOverride := entry.SizeBytes > maximumLargeOctal
	hasPAX := pathOverride || uidOverride || gidOverride || sizeOverride
	if hasPAX && !utf8.Valid(entry.Path) {
		return headerPlan{}, invalid("volume archive non-UTF-8 path requires plain USTAR")
	}
	var payload []byte
	if pathOverride {
		payload = appendPAXRecord(payload, "path", entry.Path)
		name = ordinalPath("PaxPayload/", zeroOrdinal)
		prefix = nil
	}
	if uidOverride {
		payload = appendPAXRecord(payload, "uid", []byte(strconv.FormatUint(uint64(entry.UID), 10)))
	}
	if gidOverride {
		payload = appendPAXRecord(payload, "gid", []byte(strconv.FormatUint(uint64(entry.GID), 10)))
	}
	if sizeOverride {
		payload = appendPAXRecord(payload, "size", []byte(strconv.FormatUint(entry.SizeBytes, 10)))
	}
	physicalUID := uint64(entry.UID)
	if uidOverride {
		physicalUID = 0
	}
	physicalGID := uint64(entry.GID)
	if gidOverride {
		physicalGID = 0
	}
	physicalSize := entry.SizeBytes
	if sizeOverride {
		physicalSize = 0
	}
	typeflag := byte('0')
	if entry.Kind == EntryDirectory {
		typeflag = '5'
	}
	member, err := marshalHeader(header{
		name: name, prefix: prefix, mode: uint64(entry.Mode), uid: physicalUID,
		gid: physicalGID, size: physicalSize, typeflag: typeflag,
	})
	if err != nil {
		return headerPlan{}, err
	}
	plan := headerPlan{hasPAX: hasPAX, paxPayload: payload, memberHeader: member}
	if hasPAX {
		if len(payload) > maxPAXPayload {
			return headerPlan{}, invalid("volume PAX payload exceeds its limit")
		}
		plan.paxHeader, err = marshalHeader(header{
			name: ordinalPath("PaxHeaders/", zeroOrdinal), size: uint64(len(payload)), typeflag: 'x',
		})
		if err != nil {
			return headerPlan{}, err
		}
	}
	return plan, nil
}

func appendPAXRecord(destination []byte, key string, value []byte) []byte {
	bodyLength := len(key) + 1 + len(value) + 1
	total := bodyLength + 2
	for {
		next := len(strconv.Itoa(total)) + 1 + bodyLength
		if next == total {
			break
		}
		total = next
	}
	destination = strconv.AppendInt(destination, int64(total), 10)
	destination = append(destination, ' ')
	destination = append(destination, key...)
	destination = append(destination, '=')
	destination = append(destination, value...)
	return append(destination, '\n')
}

func parsePAX(payload []byte) (paxValues, error) {
	if len(payload) == 0 || len(payload) > maxPAXPayload {
		return paxValues{}, invalid("volume PAX payload length is invalid")
	}
	var result paxValues
	for offset := 0; offset < len(payload); {
		relativeSpace := bytes.IndexByte(payload[offset:], ' ')
		if relativeSpace <= 0 {
			return paxValues{}, invalid("volume PAX record length is invalid")
		}
		space := offset + relativeSpace
		lengthBytes := payload[offset:space]
		if len(lengthBytes) > 1 && lengthBytes[0] == '0' {
			return paxValues{}, invalid("volume PAX record length has a leading zero")
		}
		length, err := strconv.ParseUint(string(lengthBytes), 10, 32)
		if err != nil || length == 0 || length > uint64(len(payload)-offset) {
			return paxValues{}, invalid("volume PAX record length is invalid")
		}
		end := offset + int(length)
		if end <= space+1 || payload[end-1] != '\n' {
			return paxValues{}, invalid("volume PAX record terminator is invalid")
		}
		record := payload[space+1 : end-1]
		equals := bytes.IndexByte(record, '=')
		if equals <= 0 {
			return paxValues{}, invalid("volume PAX record assignment is invalid")
		}
		key := string(record[:equals])
		value := record[equals+1:]
		switch key {
		case "path":
			if result.hasPath || len(value) == 0 || !utf8.Valid(value) {
				return paxValues{}, invalid("volume PAX path is invalid")
			}
			result.hasPath = true
			result.path = append([]byte(nil), value...)
		case "uid":
			if result.hasUID {
				return paxValues{}, invalid("volume PAX uid is duplicated")
			}
			parsed, parseErr := parseDecimal(value, 32)
			if parseErr != nil {
				return paxValues{}, parseErr
			}
			result.hasUID = true
			result.uid = uint32(parsed)
		case "gid":
			if result.hasGID {
				return paxValues{}, invalid("volume PAX gid is duplicated")
			}
			parsed, parseErr := parseDecimal(value, 32)
			if parseErr != nil {
				return paxValues{}, parseErr
			}
			result.hasGID = true
			result.gid = uint32(parsed)
		case "size":
			if result.hasSize {
				return paxValues{}, invalid("volume PAX size is duplicated")
			}
			parsed, parseErr := parseDecimal(value, 64)
			if parseErr != nil {
				return paxValues{}, parseErr
			}
			result.hasSize = true
			result.size = parsed
		default:
			return paxValues{}, invalid("volume PAX key is not allowed")
		}
		offset = end
	}
	return result, nil
}

func parseDecimal(value []byte, bits int) (uint64, error) {
	if len(value) == 0 || (len(value) > 1 && value[0] == '0') {
		return 0, invalid("volume PAX numeric value is not canonical decimal")
	}
	for _, current := range value {
		if current < '0' || current > '9' {
			return 0, invalid("volume PAX numeric value is not canonical decimal")
		}
	}
	parsed, err := strconv.ParseUint(string(value), 10, bits)
	if err != nil {
		return 0, invalid("volume PAX numeric value overflows")
	}
	return parsed, nil
}

func entryFromHeader(value header, pax *paxValues) (Entry, error) {
	path := physicalPath(value)
	uid, gid, size := value.uid, value.gid, value.size
	if pax != nil {
		if pax.hasPath {
			path = append([]byte(nil), pax.path...)
		}
		if pax.hasUID {
			uid = uint64(pax.uid)
		}
		if pax.hasGID {
			gid = uint64(pax.gid)
		}
		if pax.hasSize {
			size = pax.size
		}
	}
	if value.mode > maximumMode || uid > uint64(^uint32(0)) || gid > uint64(^uint32(0)) {
		return Entry{}, invalid("volume archive entry metadata overflows")
	}
	var kind EntryKind
	switch value.typeflag {
	case '0':
		kind = EntryRegular
	case '5':
		kind = EntryDirectory
	default:
		return Entry{}, invalid("volume archive typeflag is not allowed")
	}
	return Entry{
		Path: path, Kind: kind, Mode: uint32(value.mode), UID: uint32(uid), GID: uint32(gid), SizeBytes: size,
	}, nil
}

func ordinalPath(prefix string, ordinal uint64) []byte {
	digits := strconv.FormatUint(ordinal, 10)
	result := make([]byte, 0, len(prefix)+12)
	result = append(result, prefix...)
	for count := len(digits); count < 12; count++ {
		result = append(result, '0')
	}
	return append(result, digits...)
}
