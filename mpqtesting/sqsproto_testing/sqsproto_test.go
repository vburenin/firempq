package main

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	gqa "github.com/vburenin/firempq/server/sqsproto/get_queue_attributes"

	. "github.com/smartystreets/goconvey/convey"
)

func initSession() *session.Session {
	s, _ := session.NewSession(aws.NewConfig().WithEndpoint("http://127.0.0.1:8333").WithRegion("us-west-2"))
	return s
}

func initSQSClient() *sqs.SQS {
	return sqs.New(initSession())
}

func deleteQueue(s *sqs.SQS, t *testing.T, queueName string) {
	resp, err := s.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(queueName)})
	if err != nil {
		So(err.Error(), ShouldContainSubstring, "AWS.SimpleQueueService.NonExistentQueue")
	} else {
		_, e := s.DeleteQueue(&sqs.DeleteQueueInput{QueueUrl: resp.QueueUrl})
		So(e, ShouldBeNil)
	}
}

func TestCreateAndDeleteQueues(t *testing.T) {
	s := initSQSClient()
	Convey("Queues should be created and removed", t, func() {

		Convey("Create and delete queue with default parameters", func() {
			qn := "queue_name"
			deleteQueue(s, t, qn)
			resp, err := s.CreateQueue(&sqs.CreateQueueInput{QueueName: &qn})

			So(err, ShouldBeNil)
			So(resp.QueueUrl, ShouldNotBeNil)
			So(*resp.QueueUrl, ShouldContainSubstring, "/"+qn)

			deleteQueue(s, t, qn)

			So(err, ShouldBeNil)

			_, err = s.DeleteQueue(&sqs.DeleteQueueInput{
				QueueUrl: resp.QueueUrl,
			})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "AWS.SimpleQueueService.NonExistentQueue")
		})

		Convey("Create and delete queue with default parameters and test their values", func() {
			qn := "queue_name2"
			deleteQueue(s, t, qn)
			resp, err := s.CreateQueue(&sqs.CreateQueueInput{QueueName: &qn})

			So(err, ShouldBeNil)
			So(resp.QueueUrl, ShouldNotBeNil)
			So(*resp.QueueUrl, ShouldContainSubstring, "/"+qn)

			// Get All Attributes.
			respAttr, err := s.GetQueueAttributes(&sqs.GetQueueAttributesInput{
				QueueUrl:       resp.QueueUrl,
				AttributeNames: []*string{aws.String(gqa.AttrAll)},
			})
			So(err, ShouldBeNil)
			attrs := respAttr.Attributes

			So(attrs, ShouldContainKey, gqa.AttrApproximateNumberOfMessages)
			So(attrs, ShouldContainKey, gqa.AttrApproximateNumberOfMessagesNotVisible)
			So(attrs, ShouldContainKey, gqa.AttrApproximateNumberOfMessagesDelayed)

			So(attrs, ShouldContainKey, gqa.AttrVisibilityTimeout)
			So(attrs, ShouldContainKey, gqa.AttrDelaySeconds)

			So(attrs, ShouldContainKey, gqa.AttrCreatedTimestamp)
			So(attrs, ShouldContainKey, gqa.AttrLastModifiedTimestamp)

			So(attrs, ShouldContainKey, gqa.AttrMaximumMessageSize)
			So(attrs, ShouldContainKey, gqa.AttrMessageRetentionPeriod)
			So(attrs, ShouldContainKey, gqa.AttrReceiveMessageWaitTimeSeconds)
			So(attrs, ShouldContainKey, gqa.AttrQueueArn)

			So(*attrs[gqa.AttrApproximateNumberOfMessages], ShouldEqual, "0")
			So(*attrs[gqa.AttrApproximateNumberOfMessagesNotVisible], ShouldEqual, "0")
			So(*attrs[gqa.AttrApproximateNumberOfMessagesDelayed], ShouldEqual, "0")

			So(*attrs[gqa.AttrVisibilityTimeout], ShouldEqual, "60")
			So(*attrs[gqa.AttrDelaySeconds], ShouldEqual, "0")

			So(*attrs[gqa.AttrMaximumMessageSize], ShouldEqual, "262144")
			So(*attrs[gqa.AttrMessageRetentionPeriod], ShouldEqual, "345600")
			So(*attrs[gqa.AttrReceiveMessageWaitTimeSeconds], ShouldEqual, "0")
			So(*attrs[gqa.AttrQueueArn], ShouldEqual, "arn:aws:sqs:us-west-2:123456789:"+qn)

			deleteQueue(s, t, qn)

		})

	})
}
