# Fire MPQ

This project is inspired by a looking for a message priority queue service for a long time.
I didn't find anything that would be suitable enough without additional "on top" solution to
add necessary features. At some point we should stop looking to start doing something to make things done!

So that, I've started this project to help myself and people around the world to solve this problem.

Here is a list of features I am looking forward to implement.

1. Support of message priorities.
2. Messages that are sitting too long in the queue are getting auto expired and removed.
3. Delivered message is not removed from the queue immediately,
   it should be done by the consumer who received the message, otherwise this message
   will become available again after some timeout.
4. Message de-duplication. Each message may have a producer defined ID that will be used to track if there are any messages in the queue with the same id. So that message will not be added again if there is any duplicate.
5. Queue multi-casting. Message delivered into one queue, automatically will be delivered into bunch of assigned queues.
6. Service High Availability - initially, just a rollover to the slave server that will hold all replica
   from the master server. Later on, we can think about building a horizontally scalable solution.
7. Persistent messages. Once message is produced, it is guaranteed to be delivered. So, that means producer will not get back an OK response, until this message is replicated. However it should be an optional "submit confirmation" feature to satisfy different performance need.
8. Management Web-UI. A rich Web-UI that will help administrators manage existing queues, etc.
9. REDIS like protocol. So, you can use just telnet to the service and play with it.
10. REST protocol. Of course. I also would like to add SQS compatible protocol to simplify peoples life.
11. High performance. This is one of main project goals.
12. Asynchronous mode. New request can be issued without waiting for a response from a previous request. Should help a lot on a low latency network environment.

# Why Go

I just like it. I also believe that Go will improve overtime so currently existing GC issues
will be gone as well as overall performance will get increased.

# When will it be done?

I have no specific time line, as well as I do not have a lot of spare time to work on this project.
So, if you like it. Please join me! I promise you to lots of interesting algorithm challenges!
And we also can get this project done a lot faster.

# Web Site
I have registered a domain name http://firempq.com. This will be a project home page. 
