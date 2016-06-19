package validation

import (
	"fmt"
	"strings"

	"github.com/vburenin/firempq/server/sqsproto/sqserr"
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

func ValidateBatchRequestId(id string) *sqserr.SQSError {
	idLen := len(id)
	ok := true
	if idLen > 0 && idLen < 81 {
		for _, chr := range id {
			if (chr >= '0' && chr <= '9') ||
				(chr >= 'a' && chr <= 'z') ||
				(chr >= 'A' && chr <= 'Z') ||
				chr == '_' || chr == '-' {
				continue
			}
			ok = false
			break
		}
	}
	if ok {
		return nil
	}
	return sqserr.Error400(
		"AWS.SimpleQueueService.InvalidBatchEntryId",
		"A batch entry id can only contain alphanumeric characters, hyphens and underscores. "+
			"It can be at most 80 letters long.")
}

type BatchIdValidation struct {
	uniqIds map[string]struct{}
}

func NewBatchIdValidation(reqSize int) (*BatchIdValidation, *sqserr.SQSError) {
	if reqSize == 0 {
		return nil, sqserr.EmptyBatchRequestError()
	}

	if reqSize > 10 {
		return nil, sqserr.TooManyEntriesInBatchRequestError()
	}
	return &BatchIdValidation{
		uniqIds: make(map[string]struct{}, reqSize),
	}, nil
}

func (self *BatchIdValidation) Validate(id string) *sqserr.SQSError {
	if err := ValidateBatchRequestId(id); err != nil {
		return err
	}
	if _, ok := self.uniqIds[id]; ok {
		return sqserr.NotDistinctIdsError(id)
	}
	self.uniqIds[id] = struct{}{}
	return nil
}
