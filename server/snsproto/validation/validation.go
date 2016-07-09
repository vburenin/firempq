package validation

func ValidateTopicName(id string) bool {
	idLen := len(id)
	if idLen > 0 && idLen <= 256 {
		for _, chr := range id {
			if (chr >= '0' && chr <= '9') ||
				(chr >= 'a' && chr <= 'z') ||
				(chr >= 'A' && chr <= 'Z') ||
				chr == '_' || chr == '-' {
				continue
			}
			return false
		}
	}
	return true
}
