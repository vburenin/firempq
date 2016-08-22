package main

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	cq "github.com/vburenin/firempq/server/sqsproto/create_queue"
	gqa "github.com/vburenin/firempq/server/sqsproto/get_queue_attributes"

	"github.com/aws/aws-sdk-go/aws/credentials"
	. "github.com/smartystreets/goconvey/convey"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letters = []byte("abcdefghijklmnopqrstuvwxyz01234567890")

func getQueueName(qn string) string {
	res := make([]byte, 10)
	for i := 0; i < len(res); i++ {
		res[i] = letters[rand.Intn(len(letters))]
	}

	return "sqsprototest_" + string(res) + "_" + qn
}

func initSession() *session.Session {
	s, _ := session.NewSession(aws.NewConfig().WithEndpoint("http://127.0.0.1:8333").WithRegion(
		"us-west-2").WithCredentials(credentials.AnonymousCredentials))
	return s
}

func initSQSClient() *sqs.SQS {
	//sc := session.New(aws.NewConfig().WithRegion("us-west-2"))
	//s := sqs.New(sc)
	//return s
	return sqs.New(initSession())
}

func deleteQueue(s *sqs.SQS, queueName string) {
	resp, err := s.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(queueName)})
	if err != nil {
		So(err.Error(), ShouldContainSubstring, "AWS.SimpleQueueService.NonExistentQueue")
	} else {
		_, e := s.DeleteQueue(&sqs.DeleteQueueInput{QueueUrl: resp.QueueUrl})
		So(e, ShouldBeNil)
	}
}

func createDefaultQueue(s *sqs.SQS, queueName string, attrs map[string]string) *string {
	queueAttrs := map[string]*string{
		cq.AttrVisibilityTimeout:             aws.String("10"),
		cq.AttrDelaySeconds:                  aws.String("0"),
		cq.AttrMaximumMessageSize:            aws.String("123456"),
		cq.AttrMessageRetentionPeriod:        aws.String("300"),
		cq.AttrReceiveMessageWaitTimeSeconds: aws.String("15"),
	}
	for k, v := range attrs {
		queueAttrs[k] = aws.String(v)
	}

	resp, err := s.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  &queueName,
		Attributes: queueAttrs,
	})

	So(err, ShouldBeNil)
	So(resp.QueueUrl, ShouldNotBeNil)
	So(*resp.QueueUrl, ShouldContainSubstring, "/"+queueName)
	return resp.QueueUrl
}

func checkQueueSize(s *sqs.SQS, queueURL *string, avail, inFlight, delayed int64) {
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
			qn := getQueueName("queue_name")
			deleteQueue(s, qn)
			defer deleteQueue(s, qn)
			resp, err := s.CreateQueue(&sqs.CreateQueueInput{QueueName: &qn})

			So(err, ShouldBeNil)
			So(resp.QueueUrl, ShouldNotBeNil)
			So(*resp.QueueUrl, ShouldContainSubstring, "/"+qn)

			deleteQueue(s, qn)

			So(err, ShouldBeNil)

			_, err = s.DeleteQueue(&sqs.DeleteQueueInput{
				QueueUrl: resp.QueueUrl,
			})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "AWS.SimpleQueueService.NonExistentQueue")
		})

		Convey("Create and delete queue with default parameters and test their values", func() {
			ts := time.Now().Unix()
			qn := getQueueName("queue_name2")
			deleteQueue(s, qn)
			defer deleteQueue(s, qn)
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
			qn := getQueueName("queue_name3")
			deleteQueue(s, qn)
			defer deleteQueue(s, qn)
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
			parts := strings.Split(*attrs[gqa.AttrQueueArn], ":")
			So(len(parts), ShouldEqual, 6)
			So(parts[0], ShouldEqual, "arn")
			So(parts[1], ShouldEqual, "aws")
			So(parts[2], ShouldEqual, "sqs")
			So(parts[5], ShouldEqual, qn)

			modTS, _ := strconv.ParseInt(*attrs[gqa.AttrLastModifiedTimestamp], 10, 0)
			crtTS, _ := strconv.ParseInt(*attrs[gqa.AttrCreatedTimestamp], 10, 0)
			So(modTS, ShouldBeGreaterThanOrEqualTo, ts)
			So(crtTS, ShouldBeGreaterThanOrEqualTo, ts)

			So(modTS, ShouldBeLessThanOrEqualTo, ts+3600)
			So(crtTS, ShouldBeLessThanOrEqualTo, ts+3600)

		})

		Convey("Create queue, read selected parameters", func() {
			qn := getQueueName("queue_name4")
			deleteQueue(s, qn)
			defer deleteQueue(s, qn)
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
			qn := getQueueName("queue_name5")
			deleteQueue(s, qn)
			defer deleteQueue(s, qn)
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
					cq.AttrDelaySeconds:                  aws.String("8"), // different
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
			qn := getQueueName("queue_name_sent_1")
			deleteQueue(s, qn)
			defer deleteQueue(s, qn)

			queueURL := createDefaultQueue(s, qn, nil)

			sresp, err := s.SendMessage(&sqs.SendMessageInput{
				QueueUrl:    queueURL,
				MessageBody: aws.String("message data"),
			})

			So(err, ShouldBeNil)
			So(sresp.MD5OfMessageAttributes, ShouldBeNil)
			So(sresp.MD5OfMessageBody, ShouldNotBeNil)
			So(sresp.MessageId, ShouldNotBeNil)
			So(*sresp.MD5OfMessageBody, ShouldEqual, "4605064d9e2ce534ff7259b3872ce05e")

			checkQueueSize(s, queueURL, 1, 0, 0)

			msg, err := s.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl:        queueURL,
				WaitTimeSeconds: aws.Int64(10),
			})
			So(err, ShouldBeNil)

			m := msg.Messages[0]
			So(len(msg.Messages), ShouldEqual, 1)
			So(len(m.MessageAttributes), ShouldEqual, 0)
			So(m.MD5OfMessageAttributes, ShouldBeNil)
			So(*m.MD5OfBody, ShouldEqual, "4605064d9e2ce534ff7259b3872ce05e")

			checkQueueSize(s, queueURL, 0, 1, 0)

			_, err = s.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      queueURL,
				ReceiptHandle: m.ReceiptHandle,
			})
			So(err, ShouldBeNil)
			checkQueueSize(s, queueURL, 0, 0, 0)
		})

		Convey("Message with binary attribute should go through with correct checksums.", func() {
			//sc := session.New(aws.NewConfig().WithRegion("us-west-2"))
			//s := sqs.New(sc)
			qn := getQueueName("queue_name_sent_2")
			defer deleteQueue(s, qn)
			qurl := createDefaultQueue(s, qn, nil)
			mresp1, err := s.SendMessage(&sqs.SendMessageInput{
				QueueUrl:    qurl,
				MessageBody: aws.String("msgbody"),
				MessageAttributes: map[string]*sqs.MessageAttributeValue{
					"attr1": {
						BinaryValue: []byte("testval"),
						DataType:    aws.String("Binary"),
					},
				},
			})

			So(err, ShouldBeNil)
			So(mresp1.MD5OfMessageAttributes, ShouldNotBeNil)
			So(*mresp1.MD5OfMessageAttributes, ShouldEqual, "4c5f0f288e31bde6110247584f3e3184")
			So(*mresp1.MD5OfMessageBody, ShouldEqual, "a1b8f510903aa078f68f3cca9259d698")

			mresp2, err := s.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl:              qurl,
				MessageAttributeNames: []*string{aws.String("attr1")},
			})
			So(err, ShouldBeNil)
			So(len(mresp2.Messages), ShouldEqual, 1)
			m := mresp2.Messages[0]
			So(*m.MD5OfBody, ShouldEqual, "a1b8f510903aa078f68f3cca9259d698")
			So(len(m.MessageAttributes), ShouldEqual, 1)

			So(*m.MD5OfMessageAttributes, ShouldEqual, "4c5f0f288e31bde6110247584f3e3184")
			So(string(m.MessageAttributes["attr1"].BinaryValue), ShouldEqual, "testval")
			So(*m.MessageAttributes["attr1"].DataType, ShouldEqual, "Binary")
		})

		Convey("Message with string attribute should go through with correct checksums.", func() {
			qn := getQueueName("queue_name_sent_3")
			defer deleteQueue(s, qn)

			qurl := createDefaultQueue(s, qn, nil)
			mresp1, err := s.SendMessage(&sqs.SendMessageInput{
				QueueUrl:    qurl,
				MessageBody: aws.String("msgbody other"),
				MessageAttributes: map[string]*sqs.MessageAttributeValue{
					"attr1": {
						StringValue: aws.String("123"),
						DataType:    aws.String("String"),
					},
				},
			})

			So(err, ShouldBeNil)
			So(mresp1.MD5OfMessageAttributes, ShouldNotBeNil)
			So(*mresp1.MD5OfMessageAttributes, ShouldEqual, "0818c2fea90c378a0ad16b5074b9da0a")
			So(*mresp1.MD5OfMessageBody, ShouldEqual, "22b52606e4dfb6c58de7496cf34d68a2")

			mresp2, err := s.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl:              qurl,
				MessageAttributeNames: []*string{aws.String("attr1")},
			})
			So(err, ShouldBeNil)
			So(len(mresp2.Messages), ShouldEqual, 1)
			m := mresp2.Messages[0]
			So(*m.MD5OfBody, ShouldEqual, "22b52606e4dfb6c58de7496cf34d68a2")
			So(len(m.MessageAttributes), ShouldEqual, 1)

			So(*m.MD5OfMessageAttributes, ShouldEqual, "0818c2fea90c378a0ad16b5074b9da0a")
			So(*m.MessageAttributes["attr1"].StringValue, ShouldEqual, "123")
			So(*m.MessageAttributes["attr1"].DataType, ShouldEqual, "String")
		})

		Convey("Message with numberic attribute should go through with correct checksums.", func() {
			qn := getQueueName("queue_name_sent_3")
			defer deleteQueue(s, qn)

			qurl := createDefaultQueue(s, qn, nil)
			mresp1, err := s.SendMessage(&sqs.SendMessageInput{
				QueueUrl:    qurl,
				MessageBody: aws.String("msgbody other"),
				MessageAttributes: map[string]*sqs.MessageAttributeValue{
					"attr1": {
						StringValue: aws.String("10"),
						DataType:    aws.String("Number"),
					},
				},
			})

			So(err, ShouldBeNil)
			So(mresp1.MD5OfMessageAttributes, ShouldNotBeNil)
			So(*mresp1.MD5OfMessageAttributes, ShouldEqual, "025ac14d86d6d23d7f2fd1a51ecada42")
			So(*mresp1.MD5OfMessageBody, ShouldEqual, "22b52606e4dfb6c58de7496cf34d68a2")

			mresp2, err := s.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl:              qurl,
				MessageAttributeNames: []*string{aws.String("attr1")},
			})
			So(err, ShouldBeNil)
			So(len(mresp2.Messages), ShouldEqual, 1)
			m := mresp2.Messages[0]
			So(*m.MD5OfBody, ShouldEqual, "22b52606e4dfb6c58de7496cf34d68a2")
			So(len(m.MessageAttributes), ShouldEqual, 1)

			So(*m.MD5OfMessageAttributes, ShouldEqual, "025ac14d86d6d23d7f2fd1a51ecada42")
			So(*m.MessageAttributes["attr1"].StringValue, ShouldEqual, "10")
			So(*m.MessageAttributes["attr1"].DataType, ShouldEqual, "Number")
		})
	})
}
