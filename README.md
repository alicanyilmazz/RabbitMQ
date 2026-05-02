
# RabbitMQ

<img width="1692" height="930" alt="789ChatGPT Image May 2, 2026, 05_51_21 PM" src="https://github.com/user-attachments/assets/6aac4c60-b30a-4f32-b28f-099a144a14c1" />

> Docker Command
```bash
docker run -d --hostname rabbitmq-host --name rabbitmq-container -p 5672:5672 -p 15672:15672 rabbitmq:management
```

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
