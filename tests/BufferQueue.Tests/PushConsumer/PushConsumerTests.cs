// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using BufferQueue.PushConsumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace BufferQueue.Tests.PushConsumer;

public class PushConsumerTests
{
    private static readonly CountdownEvent _foo1CountdownEvent = new(105);
    private static readonly CountdownEvent _foo2CountdownEvent = new(301);
    private static readonly CountdownEvent _barCountdownEvent = new(105);

    [Fact]
    public async Task Customers_Should_Be_Able_To_Consume_Messages()
    {
        var services = new ServiceCollection();
        services
            .AddSingleton<ILogger<BufferPushCustomerHostService>>(NullLogger<BufferPushCustomerHostService>.Instance)
            .AddBufferQueue(
                bufferOptionsBuilder =>
                {
                    bufferOptionsBuilder.UseMemory(memoryBufferOptionsBuilder =>
                        {
                            memoryBufferOptionsBuilder.AddTopic<Foo>(options =>
                            {
                                options.TopicName = "topic-foo1";
                                options.PartitionNumber = 2;
                            });
                            memoryBufferOptionsBuilder.AddTopic<Foo>(options =>
                            {
                                options.TopicName = "topic-foo2";
                                options.PartitionNumber = 3;
                            });
                            memoryBufferOptionsBuilder.AddTopic<Bar>(options =>
                            {
                                options.TopicName = "topic-bar";
                                options.PartitionNumber = 2;
                            });
                        })
                        .AddPushCustomers(typeof(PushConsumerTests).Assembly);
                }
            );

        var serviceProvider = services.BuildServiceProvider();

        var bufferQueue = serviceProvider.GetRequiredService<IBufferQueue>();

        var foo1Producer = bufferQueue.GetProducer<Foo>("topic-foo1");

        for (var i = 0; i < 105; i++)
        {
            await foo1Producer.ProduceAsync(new Foo { Id = i });
        }

        var foo2Producer = bufferQueue.GetProducer<Foo>("topic-foo2");

        for (var i = 0; i < 301; i++)
        {
            await foo2Producer.ProduceAsync(new Foo { Id = i });
        }

        var barProducer = bufferQueue.GetProducer<Bar>("topic-bar");

        for (var i = 0; i < 105; i++)
        {
            await barProducer.ProduceAsync(new Bar { Id = i });
        }

        var host = serviceProvider.GetRequiredService<IHostedService>();

        _ = Task.Run(() => host.StartAsync(CancellationToken.None));

        _foo1CountdownEvent.Wait();
        _foo2CountdownEvent.Wait();
        _barCountdownEvent.Wait();

        await host.StopAsync(CancellationToken.None);
    }

    public class Foo
    {
        public int Id { get; set; }
    }

    public class Bar
    {
        public int Id { get; set; }
    }

    [BufferPushCustomer("topic-foo1", "group-foo1", 100, ServiceLifetime.Singleton, 2)]
    public class Foo1Consumer : IBufferAutoCommitPushConsumer<Foo>
    {
        public Task ConsumeAsync(IEnumerable<Foo> buffer, CancellationToken cancellationToken)
        {
            _foo1CountdownEvent.Signal(buffer.Count());
            return Task.CompletedTask;
        }
    }

    [BufferPushCustomer("topic-foo2", "group-foo2", 100, ServiceLifetime.Scoped, 2)]
    public class Foo2Consumer : IBufferManualCommitPushConsumer<Foo>
    {
        public async Task ConsumeAsync(
            IEnumerable<Foo> buffer,
            IBufferConsumerCommitter committer,
            CancellationToken cancellationToken)
        {
            var valueTask = committer.CommitAsync();
            if (!valueTask.IsCompletedSuccessfully)
            {
                await valueTask.AsTask();
            }

            _foo2CountdownEvent.Signal(buffer.Count());
        }
    }

    [BufferPushCustomer("topic-bar", "group-bar", 100, ServiceLifetime.Transient, 2)]
    public class BarConsumer : IBufferAutoCommitPushConsumer<Bar>
    {
        public Task ConsumeAsync(IEnumerable<Bar> buffer, CancellationToken cancellationToken)
        {
            _barCountdownEvent.Signal(buffer.Count());
            return Task.CompletedTask;
        }
    }
}
