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