package main

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	cfg := aws.NewConfig().WithRegion("us-west-2")
	ses := session.New(cfg)
	s := sqs.New(ses)

	url, e := s.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String("vb_test_sq1"),
	})
	if e != nil {
		log.Fatalf("Error: %v", e)
	}

	p := &sqs.AddPermissionInput{
		QueueUrl:      url.QueueUrl,
		Actions:       aws.StringSlice([]string{"ReceiveMessage"}),
		AWSAccountIds: aws.StringSlice([]string{"722908396484"}),
		Label:         aws.String("some label"),
	}
	_, e = s.AddPermission(p)

	if e != nil {
		log.Println(e.Error())
	}
}
