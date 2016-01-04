# Fire MPQ

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

Other use cases:
1. AWS SQS protocol support. Development on AWS may be costly if it involves generation of hundreds of millions of SQS messages, being able to use FireMPQ instead can reduce a cost.
2. Message content visibility. For debugging/informational purposes it may be interesting to see message content that is pending in the queue.
3. Queue content clean up. Especially in development/test environment, it is useful to clean up queue content.
4. Delivery of failed to process messages into separate queue. It is important to pin down the root cause of the processing error that message has cause, so message that exceeded the number of POPLOCK attempts should be automatically moved into error queue defined by Queue configuration.
5. AMPQ support - very long term plan if there is nothing to do. Volunteers are very welcome. May help during transition from services like RabbitMQ to FireMPQ.


Here is a list of features I am looking forward to implement the first release.

0. Simple text protocol.
1. Priorities for messages (per message).
2. Auto-expiration of messages (per message) by timeout or number of failed attempt to process it.
3. Confirmation of processing - not processed or failed to be process messages will be returned to the queue.
4. De-duplication by provided user defined message id.
5. Asynchronous requests.
6. Confirmation that message is stored on disk.

Further plans
0. Performance optimizations if possible.
1. Management Web-UI. A rich Web-UI that will help administrators manage existing queues, etc.
2. Support of AWS SQS protocol.
3. Ability to override message payload, priority and other parameters.

# When will it be done?

It is actually already working. Extensive testing is in process.

# Web Site
I have registered a domain name http://firempq.com. This will be a project home page. It is under construction at the moment.
