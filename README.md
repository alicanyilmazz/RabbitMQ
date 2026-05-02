
# RabbitMQ

<img width="1536" height="1024" alt="ope" src="https://github.com/user-attachments/assets/c95a140e-65be-46f0-b17e-96b5808ae452" />



This example demonstrates how to publish a simple message to a RabbitMQ queue using the official `RabbitMQ.Client` library in .NET.

The application connects to a local RabbitMQ instance, declares a queue named `hello-queue`, and sends a `"Hello World"` message to that queue.

## Prerequisites

Before running this example, make sure RabbitMQ is running locally.

If you are using Docker, you can start RabbitMQ with the Management UI enabled by running:

```bash
docker run -d --hostname rabbitmq-host --name rabbitmq-container -p 5672:5672 -p 15672:15672 rabbitmq:management
```

Username: guest
Password: guest

```bash
var factory = new ConnectionFactory
{
    Uri = new Uri("amqp://guest:guest@localhost:5672/")
};
```

| Key       | Description                          |
|-----------|--------------------------------------|
| AMQP      | Protocol used by RabbitMQ clients    |
| guest     | Username                             |
| guest     | Password                             |
| localhost | RabbitMQ server address              |
| 5672      | AMQP port                            |


The ConnectionFactory is responsible for creating a connection to RabbitMQ.

> Publisher

```csharp
using System.Text;
using RabbitMQ.Client;

var factory = new ConnectionFactory
{
    Uri = new Uri("amqp://guest:guest@localhost:5672/")
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(
    queue: "hello-queue",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null
);

var message = "Hello World";
var body = Encoding.UTF8.GetBytes(message);

await channel.BasicPublishAsync(
    exchange: string.Empty,
    routingKey: "hello-queue",
    body: body
);

Console.WriteLine($"Message sent: {message}");
```

> Subscriber

```csharp
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory
{
    Uri = new Uri("amqp://guest:guest@localhost:5672/")
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(
    queue: "hello-queue",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null
);

var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += async (sender, eventArgs) =>
{
    var body = eventArgs.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);

    Console.WriteLine($"Message received: {message}");

    await Task.CompletedTask;
};

await channel.BasicConsumeAsync(
    queue: "hello-queue",
    autoAck: true,
    consumer: consumer
);

Console.WriteLine("Subscriber is waiting for messages...");
Console.ReadLine();
```
