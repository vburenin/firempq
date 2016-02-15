package parsers

const (
	MaxItemIdLength      = 256
	MaxServiceNameLength = 80
)

// ValidateItemId checks if symbol range is in the allowed scope for the item id
func ValidateItemId(itemId string) bool {
	if len(itemId) == 0 || len(itemId) > MaxItemIdLength {
		return false
	}
	for _, chr := range itemId {
		if (chr >= '0' && chr <= '9') ||
			(chr >= 'a' && chr <= 'z') ||
			(chr >= 'A' && chr <= 'Z') ||
			chr == '_' || chr == '-' {
			continue
		}
		return false
	}
	return true
}

// ValidateUserItemId checks if symbol range is in the allowed scope for the item id
// User defined ID can not start with the underscore.
func ValidateUserItemId(itemId string) bool {
	if ValidateItemId(itemId) && itemId[0] != '_' {
		return true
	}
	return false
}

func ValidateServiceName(svcName string) bool {
	if len(svcName) > MaxServiceNameLength {
		return false
	}
	return ValidateItemId(svcName)
}
