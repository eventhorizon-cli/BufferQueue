using BufferQueue;
using BufferQueue.PushConsumer;
using WebApp;

namespace WebAPI;

[BufferPushCustomer(
    topicName: "topic-bar",
    groupName: "group-bar",
    batchSize: 100,
    serviceLifetime: ServiceLifetime.Scoped,
    concurrency: 2)]
public class BarPushConsumer(ILogger<BarPushConsumer> logger) : IBufferManualCommitPushConsumer<Bar>
{
    public async Task ConsumeAsync(IEnumerable<Bar> buffer, IBufferConsumerCommitter committer,
        CancellationToken cancellationToken)
    {
        foreach (var bar in buffer)
        {
            logger.LogInformation("BarPushConsumer.ConsumeAsync: {Bar}", bar);
        }

        await committer.CommitAsync();
    }
}
