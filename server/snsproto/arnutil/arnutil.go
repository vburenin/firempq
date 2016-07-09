package arnutil

import (
	"crypto/md5"
	"fmt"
	"strings"
)

func MakeTargetArn(topicArn, protocol, endpoint string) string {
	m := md5.New()
	m.Write([]byte(topicArn))
	m.Write([]byte(protocol))
	m.Write([]byte(endpoint))

	u := m.Sum(nil)
	return fmt.Sprintf("%s:%x-%x-%x-%x-%x", topicArn, u[0:4], u[4:6], u[6:8], u[8:10], u[10:])
}

const ArnPrefix = "arn:aws:sns:us-west-2:123456789012:"

func MakeTopicArn(topicName string) string {
	return ArnPrefix + topicName
}

func CutTopicName(topicArn string) string {
	return strings.TrimLeft(topicArn, ArnPrefix)
}
