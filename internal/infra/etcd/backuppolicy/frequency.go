package backuppolicy

import "github.com/AlanD20/groundplane/pkg/errs"

// MaximumBackupPolicySources is the largest selection whose worst-case
// protected replacement fits the fixed 96-operation etcd transaction budget.
// The worst case is an enabled Connector move that creates era 1 and selects
// only Attach/Volume sources: 24 fixed operations plus 6 per source.
const (
	MaximumBackupPolicySources = 12
)

func ValidateFrequency(frequency string) error {
	calendar := frequency
	if len(frequency) == 18 {
		if frequency[3] != ' ' || !validBackupPolicyWeekday(frequency[:3]) {
			return errs.New(errs.KindValidationFailed, "backup policy frequency is invalid")
		}
		calendar = frequency[4:]
	}
	if len(calendar) != 14 || calendar[:6] != "*-*-* " {
		return errs.New(errs.KindValidationFailed, "backup policy frequency is invalid")
	}
	clock := calendar[6:]
	if clock[2] != ':' || clock[5] != ':' ||
		!twoASCIIDigits(clock[0], clock[1], 23) ||
		!twoASCIIDigits(clock[3], clock[4], 59) ||
		!twoASCIIDigits(clock[6], clock[7], 59) {
		return errs.New(errs.KindValidationFailed, "backup policy frequency is invalid")
	}
	return nil
}

func validBackupPolicyWeekday(value string) bool {
	switch value {
	case "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun":
		return true
	default:
		return false
	}
}

func twoASCIIDigits(tens byte, ones byte, maximum int) bool {
	if tens < '0' || tens > '9' || ones < '0' || ones > '9' {
		return false
	}
	return int(tens-'0')*10+int(ones-'0') <= maximum
}
