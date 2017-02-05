# FireMPQ

## So Far

FireMPQ is ready to use in non critical enviroment such as development and QA.
It can talk via FireMPQ (redis like protocol) and SQS. SNS protocol is in progress.

## Install
  
  ```go get -u github.com/vburenin/firempq```

## Run

```
bash$ firempq --sqs-address :8333
2016-08-14 15:34:53.77069: INFO conn_server.go:80 Starting FireMPQ Protocol Server at :8222
2016-08-14 15:34:53.77081: INFO conn_server.go:47 Starting SQS Protocol Server on: :8333
```

## Config

```
bash$ ./firempq --help
Usage:
  firempq [OPTIONS]

Application Options:
      --fmpq-address=                  FireMPQ native protocol. (default: :8222)
      --sqs-address=                   SQS protocol interface for FireMPQ
      --flush-interval=                Disk synchronization interval in milliseconds (default: 100)
      --data-dir=                      FireMPQ database location (default: ./fmpq-data)
      --update-interval=               Timeout and expiration check period in milliseconds (default: 100)
      --msg-ttl=                       Default message TTL for a new queue in milliseconds (default: 345600000)
      --delivery-delay=                Default message delivery delay for a new queue in milliseconds (default: 0)
      --lock-timeout=                  Default message lock/visibility timeout for a new queue in milliseconds (default: 60000)
      --pop-count-limit=               Default receive attempts limit per message for a new queue (default: 99)
      --max-queue-size=                Default max number of messages per queue (default: 100000000)
      --wait-timeout=                  Default wait timeout to receive a message for a new queue in milliseconds. (default: 0)
      --max-wait-timeout=              Limit receive wait timeout. Milliseconds. (default: 20000)
      --max-receive-batch=             Limit the number of received messages at once. (default: 10)
      --max-lock-timeout=              Max lock/visibility timeout in milliseconds (default: 43200000)
      --max-delivery-delay=            Maximum delivery delay in milliseconds. (default: 900000)
      --max-message-ttl=               Maximum message TTL for the queue. In milliseconds (default: 345600000)
      --max-message-size=              Maximum message size in bytes. (default: 262144)
      --tune-process-batch=            Batch size to process expired messages and message locks. Large number may lead to not desired service timeouts
                                       (default: 1000)
      --log-level=[debug|info|warning] Log level (default: info)
      --log-config                     Print current config values
      --profiler-address=              Enables Go profiler on the defined interface:port

Help Options:
  -h, --help                           Show this help message

exit status 255
```

## Description

FireMPQ is a message queue service that provides set of features that are not available in any other queue service implementation all together.

This is a list of my typical use cases:

1. Priorities for messages (per message) - helps to prioritize the message processing. Extremely helpful if there is a need to process some messages right NOW.
2. Limited number of pop attempts. Some messages may deliver a content that causes errors on the processing side. If error persists, the message will be automatically removed from the queue after several failed attempts.
3. De-duplication. It can happen so that the same message may be pushed into the queue multiple times, however the processing should happen only once. Instead of implementing resolution point that does de-duplication, it would be great if it happens on the queue service side.
4. Asynchronous requests. Sometimes it is important to keep number of connections low, so being able to communicated over single and persistent connection asynchronously is important to achieve high throuput. Especially in the case if there are thousands of publishers and consumers.
5. Message delivery may need to be terminated, not often, but it happens - manual user intervention, etc.
6. Message payload content may need to be updated - more information gets known over time.
7. Message priority may need to be increased or decreased, so the processing may be speed up or slowed down.
8. Message delivery may need to be delayed - some data source may get updated multiple times during the delay timeout that message refers to.
9. Message may need to be delivered without confirmation that it is processed - may be useful some times when some minor percentage of losses is acceptable by design.
10. High performance. We all want to have a high performance service, so FireMPQ goal is to provide a service that can handle hundreds of thousands operations per second.

## Other use cases:

1. AWS SQS protocol support. Development on AWS may be costly if it involves generation of hundreds of millions of SQS messages, being able to use FireMPQ instead can reduce a cost.
2. Message content visibility. For debugging/informational purposes it may be interesting to see message content that is pending in the queue.
3. Queue content clean up. Especially in development/test environment, it is useful to clean up queue content.
4. Delivery of failed to process messages into separate queue. It is important to pin down the root cause of the processing error that message has cause, so message that exceeded the number of POPLOCK attempts should be automatically moved into error queue defined by Queue configuration.
5. AMPQ support - very long term plan if there is nothing to do. Volunteers are very welcome. May help during transition from services like RabbitMQ to FireMPQ.


## First release features - Implemented

0. Simple text protocol.
1. Priorities for messages (per message).
2. Auto-expiration of messages (per message) by timeout or number of failed attempt to process it.
3. Confirmation of processing - not processed or failed to be process messages will be returned to the queue.
4. De-duplication by provided user defined message id.
5. Asynchronous requests.
6. Confirmation that message is stored on disk.
7. Support of AWS SQS protocol.
8. AWS SNS is in progress.

## Further plans
0. Performance optimizations if possible.
1. Management Web-UI. A rich Web-UI that will help administrators manage existing queues, etc.

3. Ability to override message payload, priority and other parameters.

## When will it be done?

It is actually already working. Extensive testing is in process.

## Bugs

BUGS - YOU ARE WELCOME!

## Contributions

YOU ARE MORE THAN WELCOME!
