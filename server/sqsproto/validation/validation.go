package validation

import (
	"firempq/server/sqsproto/sqserr"
	"fmt"
	"strings"
)

// ValidateMessageAttrName makes sure attribute name is ok.
// Validator enforces the same attribute name limits as AWS SQS.
func ValidateMessageAttrName(v string) *sqserr.SQSError {
	if len(v) > 255 {
		return sqserr.InvalidParameterValueError("Length of message attribute name must be less than 256 bytes.")
	}
	if len(v) == 0 {
		return sqserr.InvalidParameterValueError("The request must contain non-empty message attribute name.")
	}
	for _, chr := range v {
		if (chr >= '0' && chr <= '9') ||
			(chr >= 'a' && chr <= 'z') ||
			(chr >= 'A' && chr <= 'Z') ||
			chr == '_' || chr == '-' || chr == '.' {
			continue
		}
		return sqserr.InvalidParameterValueError("Invalid non-alphanumeric character was found in the message attribute name. Can only include alphanumeric characters, hyphens, underscores, or dots.")
	}
	return nil
}

// ValidateMessageAttrType makes sure attribute type name is ok.
// Validator enforces the same attribute type name limits as AWS SQS.
func ValidateMessageAttrType(attrName, v string) *sqserr.SQSError {
	if len(v) > 255 {
		return sqserr.InvalidParameterValueError(
			fmt.Sprintf("Length of message attribute '%s' type must be less than 256 bytes.", attrName))
	}
	if len(v) == 0 {
		return sqserr.InvalidParameterValueError(
			fmt.Sprintf("The message attribute '%s' must contain non-empty message attribute type.", attrName))
	}

	typePrefix := strings.SplitN(v, ".", 1)[0]
	if typePrefix != "String" || typePrefix != "Number" || typePrefix != "Binary" {
		return sqserr.InvalidParameterValueError(
			"The message attribute '%s' has an invalid message attribute type, the set of supported type prefixes is Binary, Number, and String.",
			attrName)
	}
	return nil
}
