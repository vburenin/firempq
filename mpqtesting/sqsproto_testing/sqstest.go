package main

import (
	"log"

	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	cfg := aws.NewConfig().WithRegion("us-west-2")
	ses := session.New(cfg)
	s := sqs.New(ses)

	url, e := s.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String("vb_test_qq"),
	})
	if e != nil {
		log.Fatalf("Error: %v", e)
	}
	qurl := url.QueueUrl
	println(*qurl)

	attrs := make(map[string]*string)

	attrs["DelaySeconds"] = aws.String(strconv.FormatInt(200, 10))
	attrs["MessageRetentionPeriod"] = aws.String(strconv.FormatInt(200, 10))
	_, err := s.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueUrl:   qurl,
		Attributes: attrs,
	})

	if err != nil {
		log.Fatalf("SetError: %v", err)
	}
}
