using BufferQueue.PushConsumer;
using WebApp;

namespace WebAPI;

[BufferPushCustomer(
    topicName: "topic-foo2",
    groupName: "group-foo2",
    batchSize: 100,
    serviceLifetime: ServiceLifetime.Singleton,
    concurrency: 2)]
public class Foo2PushConsumer(ILogger<Foo2PushConsumer> logger) : IBufferAutoCommitPushConsumer<Foo>
{
    public Task ConsumeAsync(IReadOnlyList<Foo> buffer, CancellationToken cancellationToken)
    {
        for (var i = 0; i < buffer.Count; i++)
        {
            logger.LogInformation("Foo2PushConsumer.ConsumeAsync: {Foo}", buffer[i]);
        }

        return Task.CompletedTask;
    }
}
