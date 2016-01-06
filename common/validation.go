package common

const (
	MinItemIdLength = 1
	MaxItemIdLength = 256
)

// ValidateItemId checks if symbol range is in the allowed scope for the item id
func ValidateItemId(itemId string) bool {
	if len(itemId) < MinItemIdLength || len(itemId) > MaxItemIdLength {
		return false
	}
	for _, itemId := range itemId {
		if (itemId >= '0' && itemId <= '9') ||
			(itemId >= 'a' && itemId <= 'z') ||
			(itemId >= 'A' && itemId <= 'Z') ||
			itemId == '_' {
			continue
		} else {
			return false
		}
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
