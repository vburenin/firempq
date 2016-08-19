package main

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	cq "github.com/vburenin/firempq/server/sqsproto/create_queue"
	gqa "github.com/vburenin/firempq/server/sqsproto/get_queue_attributes"

	"strconv"
	"time"

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

func checkQueueSize(s *sqs.SQS, t *testing.T, queueURL *string, avail, inFlight, delayed int64) {
	// Get All Attributes.
	respAttr, err := s.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl:       queueURL,
		AttributeNames: []*string{aws.String(gqa.AttrAll)},
	})

	So(err, ShouldBeNil)
	So(*respAttr.Attributes[gqa.AttrApproximateNumberOfMessages], ShouldEqual, strconv.FormatInt(avail, 10))
	So(*respAttr.Attributes[gqa.AttrApproximateNumberOfMessagesNotVisible], ShouldEqual, strconv.FormatInt(inFlight, 10))
	So(*respAttr.Attributes[gqa.AttrApproximateNumberOfMessagesDelayed], ShouldEqual, strconv.FormatInt(delayed, 10))
}

func TestCreateAndDeleteQueues(t *testing.T) {
	s := initSQSClient()
	Convey("Queues should be created and removed", t, func() {

		Convey("Create and delete queue with default parameters", func() {
			qn := "queue_name"
			deleteQueue(s, t, qn)
			defer deleteQueue(s, t, qn)
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
			ts := time.Now().Unix()
			qn := "queue_name2"
			deleteQueue(s, t, qn)
			defer deleteQueue(s, t, qn)
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

			modTS, _ := strconv.ParseInt(*attrs[gqa.AttrLastModifiedTimestamp], 10, 0)
			crtTS, _ := strconv.ParseInt(*attrs[gqa.AttrCreatedTimestamp], 10, 0)
			So(modTS, ShouldBeGreaterThanOrEqualTo, ts)
			So(crtTS, ShouldBeGreaterThanOrEqualTo, ts)

			So(modTS, ShouldBeLessThanOrEqualTo, ts+3600)
			So(crtTS, ShouldBeLessThanOrEqualTo, ts+3600)

		})

		Convey("Create queue, set custom parameters, read them back, they should be the same", func() {
			ts := time.Now().Unix()
			qn := "queue_name3"
			deleteQueue(s, t, qn)
			defer deleteQueue(s, t, qn)
			resp, err := s.CreateQueue(&sqs.CreateQueueInput{
				QueueName: &qn,
				Attributes: map[string]*string{
					cq.AttrVisibilityTimeout:             aws.String("1001"),
					cq.AttrDelaySeconds:                  aws.String("7"),
					cq.AttrMaximumMessageSize:            aws.String("123456"),
					cq.AttrMessageRetentionPeriod:        aws.String("7203"),
					cq.AttrReceiveMessageWaitTimeSeconds: aws.String("11"),
				},
			})
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

			So(*attrs[gqa.AttrVisibilityTimeout], ShouldEqual, "1001")
			So(*attrs[gqa.AttrDelaySeconds], ShouldEqual, "7")

			So(*attrs[gqa.AttrMaximumMessageSize], ShouldEqual, "123456")
			So(*attrs[gqa.AttrMessageRetentionPeriod], ShouldEqual, "7203")
			So(*attrs[gqa.AttrReceiveMessageWaitTimeSeconds], ShouldEqual, "11")
			So(*attrs[gqa.AttrQueueArn], ShouldEqual, "arn:aws:sqs:us-west-2:123456789:"+qn)

			modTS, _ := strconv.ParseInt(*attrs[gqa.AttrLastModifiedTimestamp], 10, 0)
			crtTS, _ := strconv.ParseInt(*attrs[gqa.AttrCreatedTimestamp], 10, 0)
			So(modTS, ShouldBeGreaterThanOrEqualTo, ts)
			So(crtTS, ShouldBeGreaterThanOrEqualTo, ts)

			So(modTS, ShouldBeLessThanOrEqualTo, ts+3600)
			So(crtTS, ShouldBeLessThanOrEqualTo, ts+3600)

		})

		Convey("Create queue, read selected parameters", func() {
			qn := "queue_name5"
			deleteQueue(s, t, qn)
			defer deleteQueue(s, t, qn)
			resp, err := s.CreateQueue(&sqs.CreateQueueInput{
				QueueName: &qn,
				Attributes: map[string]*string{
					cq.AttrVisibilityTimeout:             aws.String("1001"),
					cq.AttrDelaySeconds:                  aws.String("7"),
					cq.AttrMaximumMessageSize:            aws.String("123456"),
					cq.AttrMessageRetentionPeriod:        aws.String("7203"),
					cq.AttrReceiveMessageWaitTimeSeconds: aws.String("11"),
				},
			})
			So(err, ShouldBeNil)
			So(resp.QueueUrl, ShouldNotBeNil)
			So(*resp.QueueUrl, ShouldContainSubstring, "/"+qn)

			// Get All Attributes.
			respAttr, err := s.GetQueueAttributes(&sqs.GetQueueAttributesInput{
				QueueUrl: resp.QueueUrl,
				AttributeNames: []*string{
					aws.String(gqa.AttrApproximateNumberOfMessages),
					aws.String(gqa.AttrDelaySeconds),
				},
			})
			So(err, ShouldBeNil)
			attrs := respAttr.Attributes

			So(len(attrs), ShouldEqual, 2)
			So(attrs, ShouldContainKey, gqa.AttrApproximateNumberOfMessages)
			So(attrs, ShouldContainKey, gqa.AttrDelaySeconds)

			So(*attrs[gqa.AttrApproximateNumberOfMessages], ShouldEqual, "0")
			So(*attrs[gqa.AttrDelaySeconds], ShouldEqual, "7")
		})

		Convey("Creating queue with the same name but different parameters should fail if queue exists.", func() {
			qn := "queue_name3"
			deleteQueue(s, t, qn)
			defer deleteQueue(s, t, qn)
			resp, err := s.CreateQueue(&sqs.CreateQueueInput{
				QueueName: &qn,
				Attributes: map[string]*string{
					cq.AttrVisibilityTimeout:             aws.String("1001"),
					cq.AttrDelaySeconds:                  aws.String("7"),
					cq.AttrMaximumMessageSize:            aws.String("123456"),
					cq.AttrMessageRetentionPeriod:        aws.String("7203"),
					cq.AttrReceiveMessageWaitTimeSeconds: aws.String("11"),
				},
			})
			So(err, ShouldBeNil)
			So(resp.QueueUrl, ShouldNotBeNil)
			So(*resp.QueueUrl, ShouldContainSubstring, "/"+qn)

			resp, err = s.CreateQueue(&sqs.CreateQueueInput{
				QueueName: &qn,
				Attributes: map[string]*string{
					cq.AttrVisibilityTimeout:             aws.String("1001"),
					cq.AttrDelaySeconds:                  aws.String("7"),
					cq.AttrMaximumMessageSize:            aws.String("123456"),
					cq.AttrMessageRetentionPeriod:        aws.String("7203"),
					cq.AttrReceiveMessageWaitTimeSeconds: aws.String("11"),
				},
			})
			So(err, ShouldBeNil)
			So(resp.QueueUrl, ShouldNotBeNil)
			So(*resp.QueueUrl, ShouldContainSubstring, "/"+qn)

			resp, err = s.CreateQueue(&sqs.CreateQueueInput{
				QueueName: &qn,
				Attributes: map[string]*string{
					cq.AttrVisibilityTimeout:             aws.String("1001"),
					cq.AttrDelaySeconds:                  aws.String("8"),
					cq.AttrMaximumMessageSize:            aws.String("123456"),
					cq.AttrMessageRetentionPeriod:        aws.String("7203"),
					cq.AttrReceiveMessageWaitTimeSeconds: aws.String("11"),
				},
			})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "QueueAlreadyExists")
		})

	})
}

func TestSendReceiveDeleteReturnMessages(t *testing.T) {
	s := initSQSClient()
	Convey("Message delivery should work correctly", t, func() {
		Convey("Message should be sent into empty queue, delivered and removed", func() {
			qn := "queue_name_sent_1"
			deleteQueue(s, t, qn)
			defer deleteQueue(s, t, qn)

			resp, err := s.CreateQueue(&sqs.CreateQueueInput{
				QueueName: &qn,
				Attributes: map[string]*string{
					cq.AttrVisibilityTimeout:             aws.String("10"),
					cq.AttrDelaySeconds:                  aws.String("0"),
					cq.AttrMaximumMessageSize:            aws.String("123456"),
					cq.AttrMessageRetentionPeriod:        aws.String("300"),
					cq.AttrReceiveMessageWaitTimeSeconds: aws.String("15"),
				},
			})
			So(err, ShouldBeNil)
			So(resp.QueueUrl, ShouldNotBeNil)
			So(*resp.QueueUrl, ShouldContainSubstring, "/"+qn)

			sresp, err := s.SendMessage(&sqs.SendMessageInput{
				QueueUrl:    resp.QueueUrl,
				MessageBody: aws.String("message data"),
			})

			So(err, ShouldBeNil)
			So(sresp.MD5OfMessageAttributes, ShouldBeNil)
			So(sresp.MD5OfMessageBody, ShouldNotBeNil)
			So(sresp.MessageId, ShouldNotBeNil)
			So(*sresp.MD5OfMessageBody, ShouldEqual, "4605064d9e2ce534ff7259b3872ce05e")

			checkQueueSize(s, t, resp.QueueUrl, 1, 0, 0)

			msg, err := s.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl:        resp.QueueUrl,
				WaitTimeSeconds: aws.Int64(10),
			})
			So(err, ShouldBeNil)

			m := msg.Messages[0]
			So(len(msg.Messages), ShouldEqual, 1)
			So(len(m.MessageAttributes), ShouldEqual, 0)
			So(m.MD5OfMessageAttributes, ShouldBeNil)
			So(*m.MD5OfBody, ShouldEqual, "4605064d9e2ce534ff7259b3872ce05e")

			checkQueueSize(s, t, resp.QueueUrl, 0, 1, 0)

			_, err = s.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      resp.QueueUrl,
				ReceiptHandle: m.ReceiptHandle,
			})
			So(err, ShouldBeNil)
			checkQueueSize(s, t, resp.QueueUrl, 0, 0, 0)
		})
	})
}
