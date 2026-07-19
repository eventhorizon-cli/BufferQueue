// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;
using BufferQueue.PushConsumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace BufferQueue.Tests.PushConsumer;

public class BufferPushCustomerHostServiceLifetimeTests
{
    [Theory]
    [InlineData(ServiceLifetime.Singleton, 1, 0)]
    [InlineData(ServiceLifetime.Scoped, 3, 3)]
    [InlineData(ServiceLifetime.Transient, 3, 3)]
    public async Task ConsumeAsync_Should_Honor_Service_Lifetime_Across_Batches(
        ServiceLifetime lifetime,
        int expectedInstanceCount,
        int expectedDisposeCountBeforeProviderDispose)
    {
        var probe = new LifetimeProbe();
        var descriptor = CreateServiceDescriptor(lifetime, probe);
        IServiceCollection services = new ServiceCollection();
        services.Add(descriptor);
        var provider = services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateOnBuild = true,
            ValidateScopes = true
        });
        var (host, pullConsumer) = CreateHost(provider, descriptor);

        LifetimeSnapshot beforeProviderDispose;
        try
        {
            await host.StartAsync(CancellationToken.None);
            await pullConsumer.Completion.WaitAsync(TimeSpan.FromSeconds(5));
            await host.StopAsync(CancellationToken.None);
            beforeProviderDispose = probe.Snapshot();
        }
        finally
        {
            await provider.DisposeAsync();
        }

        Assert.Equal(new[] { 2, 2, 1 }, beforeProviderDispose.BatchSizes);
        Assert.Equal(3, beforeProviderDispose.ConsumedInstanceIds.Length);
        Assert.Equal(expectedInstanceCount, beforeProviderDispose.CreatedInstanceIds.Length);
        Assert.Equal(expectedInstanceCount, beforeProviderDispose.ConsumedInstanceIds.Distinct().Count());
        Assert.Equal(expectedDisposeCountBeforeProviderDispose,
            beforeProviderDispose.DisposedInstanceIds.Length);
        Assert.Equal(beforeProviderDispose.DisposedInstanceIds.Length,
            beforeProviderDispose.DisposedInstanceIds.Distinct().Count());
        Assert.False(beforeProviderDispose.ConsumedAfterDispose);
        Assert.False(beforeProviderDispose.DuplicateDispose);

        var afterProviderDispose = probe.Snapshot();
        Assert.Equal(expectedInstanceCount, afterProviderDispose.DisposedInstanceIds.Length);
        Assert.Equal(
            afterProviderDispose.CreatedInstanceIds.Order(),
            afterProviderDispose.DisposedInstanceIds.Order());
        Assert.False(afterProviderDispose.DuplicateDispose);
    }

    [Fact]
    public async Task Scoped_Consumer_Should_Be_Disposed_And_Recreated_After_Handler_Failure()
    {
        var probe = new LifetimeProbe(throwOnBatchNumber: 2);
        var descriptor = CreateServiceDescriptor(ServiceLifetime.Scoped, probe);
        IServiceCollection services = new ServiceCollection();
        services.Add(descriptor);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateOnBuild = true,
            ValidateScopes = true
        });
        var (host, pullConsumer) = CreateHost(provider, descriptor);

        await host.StartAsync(CancellationToken.None);
        await pullConsumer.Completion.WaitAsync(TimeSpan.FromSeconds(5));
        await host.StopAsync(CancellationToken.None);

        var snapshot = probe.Snapshot();
        Assert.Equal(new[] { 2, 2, 1 }, snapshot.BatchSizes);
        Assert.Equal(3, snapshot.CreatedInstanceIds.Length);
        Assert.Equal(3, snapshot.ConsumedInstanceIds.Distinct().Count());
        Assert.Equal(3, snapshot.DisposedInstanceIds.Length);
        Assert.Equal(3, snapshot.DisposedInstanceIds.Distinct().Count());
        Assert.False(snapshot.ConsumedAfterDispose);
        Assert.False(snapshot.DuplicateDispose);
    }

    private static ServiceDescriptor CreateServiceDescriptor(
        ServiceLifetime lifetime,
        LifetimeProbe probe) =>
        new(
            typeof(ITrackedPushConsumer),
            _ => CreatePushConsumer(probe),
            lifetime);

    private static ITrackedPushConsumer CreatePushConsumer(LifetimeProbe probe)
    {
        var consumer = DispatchProxy.Create<ITrackedPushConsumer, TrackedPushConsumerProxy>();
        ((TrackedPushConsumerProxy)(object)consumer).Initialize(probe);
        return consumer;
    }

    private static (BufferPushCustomerHostService Host, FakePullConsumer<int> PullConsumer) CreateHost(
        IServiceProvider serviceProvider,
        ServiceDescriptor descriptor)
    {
        var pullConsumer = new FakePullConsumer<int>(
        [
            [1, 2],
            [3, 4],
            [5]
        ]);
        var bufferQueue = new FakeBufferQueue(pullConsumer);
        var description = new BufferPushConsumerDescription(
            new BufferPullConsumerOptions
            {
                TopicName = "lifetime-topic",
                GroupName = "lifetime-validation",
                BatchSize = 2,
                AutoCommit = true
            },
            descriptor,
            Concurrency: 1);
        var host = new BufferPushCustomerHostService(
            bufferQueue,
            [description],
            serviceProvider,
            NullLogger<BufferPushCustomerHostService>.Instance);

        return (host, pullConsumer);
    }

    public interface ITrackedPushConsumer : IBufferAutoCommitPushConsumer<int>, IAsyncDisposable
    {
    }

    public class TrackedPushConsumerProxy : DispatchProxy
    {
        private int _disposed;
        private Guid _instanceId;
        private LifetimeProbe? _probe;

        public void Initialize(LifetimeProbe probe)
        {
            _probe = probe;
            _instanceId = Guid.NewGuid();
            probe.RecordCreated(_instanceId);
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            ArgumentNullException.ThrowIfNull(targetMethod);
            var probe = _probe ?? throw new InvalidOperationException("The proxy has not been initialized.");

            if (targetMethod.Name == nameof(IBufferAutoCommitPushConsumer<int>.ConsumeAsync))
            {
                var buffer = (IEnumerable<int>)args![0]!;
                var throwOnThisBatch = probe.RecordConsumed(
                    _instanceId,
                    buffer.Count(),
                    Volatile.Read(ref _disposed) != 0);
                return throwOnThisBatch
                    ? Task.FromException(new InvalidOperationException("Expected consumer failure."))
                    : Task.CompletedTask;
            }

            if (targetMethod.Name == nameof(IAsyncDisposable.DisposeAsync))
            {
                var duplicateDispose = Interlocked.Exchange(ref _disposed, 1) != 0;
                probe.RecordDisposed(_instanceId, duplicateDispose);
                return ValueTask.CompletedTask;
            }

            throw new NotSupportedException($"Unexpected proxy method '{targetMethod.Name}'.");
        }
    }

    public sealed class LifetimeProbe(int? throwOnBatchNumber = null)
    {
        private readonly ConcurrentQueue<int> _batchSizes = new();
        private readonly ConcurrentQueue<Guid> _consumedInstanceIds = new();
        private readonly ConcurrentQueue<Guid> _createdInstanceIds = new();
        private readonly ConcurrentQueue<Guid> _disposedInstanceIds = new();
        private int _batchNumber;
        private int _consumedAfterDispose;
        private int _duplicateDispose;

        public void RecordCreated(Guid instanceId) => _createdInstanceIds.Enqueue(instanceId);

        public bool RecordConsumed(Guid instanceId, int batchSize, bool consumedAfterDispose)
        {
            _consumedInstanceIds.Enqueue(instanceId);
            _batchSizes.Enqueue(batchSize);
            if (consumedAfterDispose)
            {
                Interlocked.Exchange(ref _consumedAfterDispose, 1);
            }

            return Interlocked.Increment(ref _batchNumber) == throwOnBatchNumber;
        }

        public void RecordDisposed(Guid instanceId, bool duplicateDispose)
        {
            _disposedInstanceIds.Enqueue(instanceId);
            if (duplicateDispose)
            {
                Interlocked.Exchange(ref _duplicateDispose, 1);
            }
        }

        public LifetimeSnapshot Snapshot() => new(
            _createdInstanceIds.ToArray(),
            _consumedInstanceIds.ToArray(),
            _disposedInstanceIds.ToArray(),
            _batchSizes.ToArray(),
            Volatile.Read(ref _consumedAfterDispose) != 0,
            Volatile.Read(ref _duplicateDispose) != 0);
    }

    public sealed record LifetimeSnapshot(
        Guid[] CreatedInstanceIds,
        Guid[] ConsumedInstanceIds,
        Guid[] DisposedInstanceIds,
        int[] BatchSizes,
        bool ConsumedAfterDispose,
        bool DuplicateDispose);

    private sealed class FakeBufferQueue(FakePullConsumer<int> pullConsumer) : IBufferQueue
    {
        public IBufferProducer<T> GetProducer<T>(string topicName) => throw new NotSupportedException();

        public IBufferPullConsumer<T> CreatePullConsumer<T>(BufferPullConsumerOptions options) =>
            GetPullConsumer<T>();

        public IEnumerable<IBufferPullConsumer<T>> CreatePullConsumers<T>(
            BufferPullConsumerOptions options,
            int consumerNumber) =>
            [GetPullConsumer<T>()];

        private IBufferPullConsumer<T> GetPullConsumer<T>() =>
            typeof(T) == typeof(int)
                ? (IBufferPullConsumer<T>)(object)pullConsumer
                : throw new NotSupportedException();
    }

    private sealed class FakePullConsumer<T>(IReadOnlyList<IEnumerable<T>> batches) : IBufferPullConsumer<T>
    {
        private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public string TopicName => "lifetime-topic";

        public string GroupName => "lifetime-validation";

        public Task Completion => _completion.Task;

        public async IAsyncEnumerable<IEnumerable<T>> ConsumeAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            try
            {
                foreach (var batch in batches)
                {
                    await Task.Yield();
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return batch;
                }
            }
            finally
            {
                _completion.TrySetResult();
            }
        }

        public ValueTask CommitAsync() => ValueTask.CompletedTask;
    }
}
