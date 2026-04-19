# RabbitMQ


RabbitMQ is a message broker.
It is used to transfer data or event messages between applications in a reliable and organized way.

In simple terms, there is a sender (publisher/producer) and a receiver (consumer/subscriber).
The sender publishes a message to RabbitMQ, and RabbitMQ routes that message to the appropriate queue and delivers it to the relevant consumer application.

RabbitMQ is written in Erlang and it is open source.
It is also largely based on the AMQP (Advanced Message Queuing Protocol) model.

There are also other messaging systems besides RabbitMQ, such as:

Azure Queue Storage
Azure Service Bus
Kafka
MSMQ

However, they do not all work in exactly the same way and they do not provide the same features.

How does RabbitMQ work?

The main components in RabbitMQ are:

Publisher / Producer: The application that creates and sends messages
Exchange: The component that routes incoming messages to the appropriate queues
Queue: The place where messages are stored
Consumer / Subscriber: The application that listens to a queue and processes messages
1) The publisher creates a message

A publisher can be any kind of application.
It may be an ASP.NET Core application, a Node.js service, a Java application, a Python service, or any other system.

2) The message is sent to an exchange

In RabbitMQ, messages are technically published to an exchange.
The role of the exchange is to route the incoming message to the correct queue or queues according to certain rules.

One common mistake is to say that the publisher sends the message directly to a queue.
Technically, in RabbitMQ, publishing is always done to an exchange.
Even in scenarios that look like direct queue delivery, the default exchange is usually involved behind the scenes.

3) The exchange routes the message to the appropriate queue(s)

An exchange can route a message:

to a single queue,
to multiple queues,
or to no queue at all.

This depends on the exchange type, bindings, and routing key rules.

If there is no matching queue or binding for the published message, the message may not be stored anywhere and can be lost.
That is why queues and bindings must be configured properly when guaranteed processing is required.

4) The consumer / subscriber listens to the queue

A consumer or subscriber connects to a queue, receives messages from it, and processes them.

These applications can also be written in different technologies.
For example, the publisher might be written in ASP.NET Core, while the consumers may be implemented in Node.js, Java, Go, or Python.

This gives great flexibility and makes communication between different systems easier.

Can there be multiple subscribers?

Yes. A single queue can be consumed by multiple consumers.

In that case, messages are typically distributed among the consumers.
For example, if three consumers listen to the same queue, incoming messages can be shared across them. This is commonly used for load balancing and scaling.

However, there is an important distinction:

If you want the same message to reach multiple different systems, you typically use multiple queues.
If you want to distribute the workload of one queue, you can have multiple consumers on the same queue.
Why is the exchange important?

In simple systems, messaging may seem straightforward, but as the system grows, using exchanges becomes much more beneficial.

Because with exchanges, you can:

route messages based on rules,
distribute one message to multiple queues,
separate different business flows,
make the system more flexible and scalable.
