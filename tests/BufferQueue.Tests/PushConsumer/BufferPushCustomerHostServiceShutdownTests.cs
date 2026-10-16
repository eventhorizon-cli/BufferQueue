using System.Runtime.CompilerServices;
using BufferQueue.PushConsumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace BufferQueue.Tests.PushConsumer;

public sealed class BufferPushCustomerHostServiceShutdownTests
{
    [Fact]
    public async Task StopAsync_Should_Wait_For_InFlight_Handler()
    {
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var descriptor = new ServiceDescriptor(
            typeof(IBlockingPushConsumer),
            _ => new BlockingPushConsumer(handlerStarted, releaseHandler, handlerCompleted),
            ServiceLifetime.Singleton);
        IServiceCollection services = new ServiceCollection();
        services.Add(descriptor);
        using var provider = services.BuildServiceProvider();
        var pullConsumer = new ReproPullConsumer<int>([[1]]);
        var host = new BufferPushCustomerHostService(
            new ReproBufferQueue(pullConsumer),
            [new BufferPushConsumerDescription(
                new BufferPullConsumerOptions
                {
                    TopicName = "topic",
                    GroupName = "group",
                    BatchSize = 1,
                    AutoCommit = true
                },
                descriptor,
                1)],
            provider,
            NullLogger<BufferPushCustomerHostService>.Instance);

        await host.StartAsync(CancellationToken.None);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var stopTask = host.StopAsync(CancellationToken.None);
        Assert.False(stopTask.IsCompleted);
        releaseHandler.SetResult();
        await stopTask.WaitAsync(TimeSpan.FromSeconds(5));
        await handlerCompleted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        host.Dispose();
    }

    private interface IBlockingPushConsumer : IBufferAutoCommitPushConsumer<int>;

    private struct BlockingPushConsumer(
        TaskCompletionSource handlerStarted,
        TaskCompletionSource releaseHandler,
        TaskCompletionSource handlerCompleted) : IBlockingPushConsumer
    {
        public async Task ConsumeAsync(IEnumerable<int> buffer, CancellationToken cancellationToken)
        {
            handlerStarted.SetResult();
            await releaseHandler.Task;
            handlerCompleted.SetResult();
        }
    }

    private sealed class ReproBufferQueue(ReproPullConsumer<int> pullConsumer) : IBufferQueue
    {
        public IBufferProducer<T> GetProducer<T>(string topicName) => throw new NotSupportedException();
        public IBufferPullConsumer<T> CreatePullConsumer<T>(BufferPullConsumerOptions options) =>
            (IBufferPullConsumer<T>)(object)pullConsumer;
        public IEnumerable<IBufferPullConsumer<T>> CreatePullConsumers<T>(BufferPullConsumerOptions options, int consumerNumber) =>
            [(IBufferPullConsumer<T>)(object)pullConsumer];
    }

    private sealed class ReproPullConsumer<T>(IReadOnlyList<IEnumerable<T>> batches) : IBufferPullConsumer<T>
    {
        public string TopicName => "topic";
        public string GroupName => "group";

        public async IAsyncEnumerable<IEnumerable<T>> ConsumeAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var batch in batches)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return batch;
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
        }

        public ValueTask CommitAsync() => ValueTask.CompletedTask;
    }
}
