BufferQueue
===========

[![codecov](https://codecov.io/gh/eventhorizon-cli/BufferQueue/graph/badge.svg?token=GYTOIKCXD5)](https://codecov.io/gh/eventhorizon-cli/BufferQueue)
[![Nuget](https://img.shields.io/nuget/v/BufferQueue)](https://www.nuget.org/packages/BufferQueue/)

English | [简体中文](./README.zh-CN.md)

BufferQueue 是一个用 .NET 编写的高性能的缓冲队列实现，支持多线程并发操作。

项目是从 [mocha](https://github.com/dotnetcore/mocha) 项目中独立出来的一个组件，经过修改以提供更通用的缓冲队列功能。

BufferQueue 当前提供两种存储模式：

- Memory：进程内分段内存存储，主要优化批量消费吞吐。
- MemoryMappedFile：本地内存映射分段文件存储，并持久化 writer offset 和 consumer offset。

## 适用场景
生产者和消费者之间的速度不一致，需要并发批量处理数据的场景。

如果只需要在当前进程内缓存数据，并且优先考虑消费吞吐，可以使用 Memory 模式。

如果生产的数据和已提交消费进度需要在进程重启后保留，可以使用 MemoryMappedFile 模式。MemoryMappedFile 模式是面向单个 active queue 实例的本地持久化机制，不负责协调多个进程同时写入同一个 topic directory。

## 和其他常用内存缓存 Queue 的对比

**BufferQueue 的核心优势在于批量消费时的高性能表现。**

项目内置 BenchmarkDotNet 基准测试，对比 `BufferQueue` 启用 Memory 模式后的 `MemoryBufferQueue<T>`、`Channel<T>` 和 `BlockingCollection<T>` 的并发生产、并发消费表现。下表展示 `Channel<T>` 对比结果摘要。

结果摘要：

- 生产：在该次记录参数下，`Channel<T>` 在 Unbounded 模式下约快 `2.38x`，在 Bounded 模式下约快 `1.69x`；不过两种 queue 完成 8192 条并发写入都处于亚毫秒级，仍属于同一数量级。
- 消费：`MemoryBufferQueue<T>` 的主要优势在批量消费；在本组测试中，批量越大优势通常越明显，该次记录参数下最高约快 `84x`。
- 内存分配：生产场景 `MemoryBufferQueue<T>` 分配较少；消费场景 `Channel<T>` 分配较少。

下面保留的是已记录 benchmark 的代表性数据。纯内存 queue 对比使用 `MessageSize = 8192`。生产数据的
Bounded 和 Unbounded 模式使用同一个 `Fixed` job，配置为 `LaunchCount = 1`、`WarmupCount = 6`、
`IterationCount = 15`，并运行于 .NET 10。本次只重新运行生产 benchmark，消费行仍保留此前记录的代表性数据。
MemoryMappedFile queue 对比使用 `MessageSize = 1024` 和 short-run job，以缩短文件存储场景的运行时间。

生产和消费的并发数均取自 `Environment.ProcessorCount`，该次记录结果为 `12`。生产场景使用 `12` 个 task
共享一个 `Channel<T>` writer 或一个 `MemoryBufferQueue<T>` producer，MemoryBufferQueue 配置 `12` 个 partition；
消费场景使用 `12` 个 Channel reader task，或在 `12` 个 partition 上使用 `12` 个 BufferQueue consumer。

| 类型 | 场景 | 参数 | `Channel<T>` | `MemoryBufferQueue<T>` | 结论 |
| --- | --- | --- | ---: | ---: | --- |
| 生产 | Unbounded | `MessageSize = 8192`, `ProducerTasks = 12` | `289.5 μs` | `688.2 μs` | `Channel<T>` 更快 |
| 生产 | Bounded | `MessageSize = 8192`, `ProducerTasks = 12` | `304.1 μs` | `512.9 μs` | `Channel<T>` 更快 |
| 消费 | Unbounded | `MessageSize = 8192`, `BatchSize = 1000`, `ConsumerTasks = 12` | `3,461.03 μs` | `41.30 μs` | 该次记录参数下约快 `84x` |
| 消费 | Bounded | `MessageSize = 8192`, `BatchSize = 1000`, `ConsumerTasks = 12` | `2,214.21 μs` | `41.68 μs` | 该次记录参数下约快 `53x` |

生产 benchmark 测试平台：

| 项目 | 信息 |
| --- | --- |
| 操作系统 | macOS `15.7.7` (`24G720`) |
| 运行时标识 | `osx-arm64` |
| .NET SDK | `10.0.100` |
| .NET runtime | `10.0.0`, `arm64` |
| BenchmarkDotNet | `0.15.8` |
| Channel | .NET `10.0.0` 共享框架自带的 `System.Threading.Channels 10.0.0`；程序集版本 `10.0.0.0`；未单独引用 NuGet 包 |
| Benchmark 运行目标 | `net10.0` |

可以通过以下命令运行完整 benchmark：

```shell
dotnet run -c Release --project tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj
```

只运行 System.Text.Json、MessagePack 和 unmanaged MMF serializer 对比：

```shell
dotnet run -c Release --project tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj -- --filter '*MemoryMappedFileSerializer*'
```

## 功能设计：
1. 支持创建多个 Topic，每个 Topic 可以有多种数据类型。每一对 Topic 和数据类型对应一个独立的缓冲区。

2. 支持创建多个 Consumer Group，每个 Consumer Group 的消费进度都是独立的。支持多个 Consumer Group 并发消费同一个 Topic。

3. 支持同一个 Consumer Group 创建多个 Consumer，以负载均衡的方式消费数据。

4. 支持数据的批量消费，可以一次性获取多条数据。

5. 支持 pull 模式和 push 模式两种消费模式。

6. pull 模式下和 push 模式下都支持 auto commit 和 manual commit 两种提交方式。auto commit 模式下，消费者在收到数据后自动提交消费进度，如果消费失败不会重试。manual commit 模式下，消费者需要手动提交消费进度，如果消费失败只要不提交进度就可以重试。

7. 支持多种存储模式。Memory 模式把数据和 offset 保存在进程内存中。MemoryMappedFile 模式把记录写入内存映射分段文件，并把 writer offset 和已提交的 consumer offset 持久化到磁盘。

![BufferQueue](docs/assets/BufferQueueMindMap.png)

## 高性能设计
### 无锁设计

生产和消费操作均为无锁操作，性能高效。

### 多 partition 设计
每个 Topic 可以有多个 partition，每个 partition 都有独立的消费进度，支持多个 Consumer Group 并发消费。

Producer 以轮询的方式往每个 Partition 中写入数据。

**Consumer 最多不允许超过 Partition 的数量，Partition 会均分到组内每个 Customer 上**。

当一个 Consumer 被分配了多个 Partition 时，以轮训的方式进行消费。

每个 Partition 上会记录不同消费组的消费进度，不同组之间的消费进度互不干扰。

![BufferQueue](docs/assets/Partition.png)

### 动态调整缓冲区大小
支持动态调整缓冲区大小，以适应生产和消费速度不断变化的场景。

## 使用示例

安装核心 NuGet 包：

```shell
dotnet add package BufferQueue
```

项目基于 Microsoft.Extensions.DependencyInjection，使用时需要先注册服务。

BufferQueue 支持两种消费模式：pull 模式和 push 模式。

### Memory 模式注册

Memory 模式把数据保存在进程内存中，并支持可选的有界容量。

```csharp
builder.Services.AddBufferQueue(bufferOptionsBuilder =>
{
    bufferOptionsBuilder
        .UseMemory(memoryBufferOptionsBuilder =>
        {
            // 每一对 Topic 和数据类型对应一个独立的缓冲区，可以设置 partitionNumber
            memoryBufferOptionsBuilder
                .AddTopic<Foo>(options =>
                {
                    options.TopicName = "topic-foo1";
                    options.PartitionNumber = 6;
                })
                .AddTopic<Foo>(options =>
                {
                    options.TopicName = "topic-foo2";
                    options.PartitionNumber = 4;
                })
                .AddTopic<Bar>(options =>
                {
                    options.TopicName = "topic-bar";
                    options.PartitionNumber = 8;
                    // 可以设置缓冲区的最大容量
                    options.BoundedCapacity = 100_000;
                });
        })
        // 添加 push 模式的消费者
        // 扫描指定程序集中的标记了 BufferPushCustomerAttribute 的类，
        // 注册为 push 模式的消费者
        .AddPushCustomers(typeof(Program).Assembly);
});

// 在 HostedService 中使用 pull模式 消费数据
builder.Services.AddHostedService<Foo1PullConsumerHostService>();
```

### MemoryMappedFile 模式注册

MemoryMappedFile 模式把序列化后的记录写入内存映射文件，并持久化 offset。默认序列化器是基于 `System.Text.Json` 的 internal 实现，也可以通过 `MemoryMappedFileBufferQueueOptions<T>.Serializer` 使用内置 MessagePack、unmanaged struct 序列化器或自定义序列化器。

MemoryMappedFile 支持通过独立包发布，使用前需要安装：

```shell
dotnet add package BufferQueue.MemoryMappedFile
```

`BufferQueue.MemoryMappedFile` 依赖核心 `BufferQueue` 包。公共 namespace 和 `.UseMemoryMappedFile(...)` 注册调用保持不变。

```csharp
builder.Services.AddBufferQueue(bufferOptionsBuilder =>
{
    bufferOptionsBuilder
        .UseMemoryMappedFile(memoryMappedFileBufferOptionsBuilder =>
        {
            memoryMappedFileBufferOptionsBuilder
                .AddTopic<Foo>(options =>
                {
                    options.TopicName = "topic-foo";
                    options.PartitionNumber = 4;
                    options.SegmentSize = 64L * 1024 * 1024;
                    options.DataDirectory = "/var/lib/bufferqueue";
                    options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
                    options.FlushBatchSize = 100;
                    options.Serializer = new MessagePackMemoryMappedFileSerializer<Foo>();
                });
        });
});
```

使用默认的 `MessagePackSerializerOptions.Standard` 时，自定义类型应声明显式 MessagePack contract，并使用稳定的数字 key：

```shell
dotnet add package MessagePack --version 3.1.8
```

应用项目应直接引用 MessagePack，以便它的 analyzer 和 source generator 在应用项目中运行；`BufferQueue.MemoryMappedFile` 包对 MessagePack 的传递 runtime 依赖不会提供这些构建资产。

```csharp
using MessagePack;

[MessagePackObject]
public sealed class Foo
{
    [Key(0)]
    public int Id { get; set; }

    [Key(1)]
    public string Name { get; set; } = string.Empty;
}
```

`MessagePackMemoryMappedFileSerializer<T>` 也可以接收 `MessagePackSerializerOptions`，用于配置 resolver、压缩或 `MessagePackSecurity.UntrustedData`。配置的 resolver 和 formatter 必须能够安全并发使用。`Serializer` 是可选配置；如果不配置，MemoryMappedFile 模式默认使用基于 `System.Text.Json` 的 internal 实现。其他格式可以通过实现 `IMemoryMappedFileSerializer<T>` 接入。

对于布局固定的 unmanaged struct，`UnmanagedMemoryMappedFileSerializer<T>` 会直接复制值的内存表示，不经过 JSON 或 MessagePack 编解码：

```csharp
using System.Runtime.InteropServices;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct Quote
{
    public long Sequence;
    public double Price;
    public int Quantity;
}

options.Serializer = UnmanagedMemoryMappedFileSerializer<Quote>.Instance;
```

`unmanaged` 约束会在编译期排除引用字段。`[StructLayout]` 不是使用要求，但对于持久化数据，建议显式固定 layout 和 packing。Native endianness、padding、字段顺序、packing、runtime 和进程架构都属于这种原始格式的一部分；应避免 pointer-sized 或进程相关字段，并且在旧记录仍然存在时不能修改结构布局。读取时会严格校验 payload 长度。该序列化器可以消除格式编解码，但当前队列契约仍会创建 payload `byte[]`，因此它不是 MMF 零拷贝读取。

配置的 serializer、数字 key、resolver、压缩及其他 wire-format options 必须与该 topic 已存储的记录保持兼容；已删除字段的数字 key 不能复用。修改持久化格式可能导致恢复后无法消费已有记录。

同一个 serializer 实例会由该 topic 的多个 partition 共享，并且可能被并发调用。自定义 `IMemoryMappedFileSerializer<T>` 实现必须是线程安全的，并且不能返回 `null`。

`FlushStrategy` 默认为 `MemoryMappedFileFlushStrategy.Immediate`，即每写入一条记录都显式 flush。`MemoryMappedFileFlushStrategy.Batch` 会在一个 partition 累积写入 `FlushBatchSize` 条记录后显式 flush；`FlushBatchSize` 默认为 `100`。无论配置的 batch size 是多少，segment rollover 和 consumer commit 都始终是强制 flush 边界。每次到达边界时，partition 都会先成功 flush 日志，再推进 `writer.offset`。

Batch flush 可以减少显式 flush 次数，但如果后续没有继续生产、segment rollover 或 consumer commit，未满一批的尾部记录不保证已经被显式 flush。要求每次成功 produce 都形成显式持久化边界的应用应使用默认的 `Immediate` 策略。

MemoryMappedFile 数据按 topic 和 partition 存储：

```text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/
```

例如：

```text
/var/lib/bufferqueue/topic-foo/partition-00000/00000000000000000000.log
/var/lib/bufferqueue/topic-foo/partition-00000/writer.offset
/var/lib/bufferqueue/topic-foo/partition-00000/offsets/group-foo/offset
```

Consumer group name 如果本身是合法文件夹名，会直接作为 offset 目录名。不能作为单个路径组件的字符会被百分号编码。例如 group `orders/worker 1` 会存储在：

```text
offsets/orders%2Fworker 1/offset
```

恢复时，如果 `writer.offset` 不存在，会从 `0` 开始扫描；如果 writer offset 落后于实际日志，会继续向后扫描补齐。由于 `writer.offset` 只会在对应日志成功 flush 后推进，它可以安全地落后于 checkpoint 之后实际写入日志的完整记录。损坏、超过日志范围、或没有对齐到 record boundary 的 offset 会直接抛出异常，不会静默重置进度。在 `Batch` 模式下，异常终止后未 flush 的尾部记录可能不存在。

恢复已有 MemoryMappedFile topic 时，不要调小 `PartitionNumber`。原本存储在被移除 partition 中的历史记录将不再有对应的 partition reader，因此无法继续消费；如果已有 partition 目录超过当前配置的 partition 数量，启动会快速失败并抛出 `InvalidDataException`。

### Pull 模式消费者

pull 模式的消费者示例：

```csharp
public class Foo1PullConsumerHostService(
    IBufferQueue bufferQueue,
    ILogger<Foo1PullConsumerHostService> logger) : IHostedService
{
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var token = CancellationTokenSource
            .CreateLinkedTokenSource(cancellationToken, _cancellationTokenSource.Token)
            .Token;

        var consumers = bufferQueue.CreatePullConsumers<Foo>(
            new BufferPullConsumerOptions
            {
                TopicName = "topic-foo1", GroupName = "group-foo1", AutoCommit = true, BatchSize = 100,
            }, consumerNumber: 4);

        foreach (var consumer in consumers)
        {
            _ = ConsumeAsync(consumer, token);
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _cancellationTokenSource.Cancel();
        return Task.CompletedTask;
    }

    private async Task ConsumeAsync(IBufferPullConsumer<Foo> consumer, CancellationToken cancellationToken)
    {
        await foreach (var buffer in consumer.ConsumeAsync(cancellationToken))
        {
            foreach (var foo in buffer)
            {
                // Process the foo
                logger.LogInformation("Foo1PullConsumerHostService.ConsumeAsync: {Foo}", foo);
            }
        }
    }
}
```

push 模式的消费者示例：

通过 BufferPushCustomer 特性注册 push 模式的消费者。

push consumer 会被注册到 DI 容器中，可以通过构造函数注入其他服务，可以通过设置 ServiceLifetime 来控制 consumer 的生命周期。

BufferPushCustomerAttribute 中的 concurrency 参数用于设置 push consumer 的消费并发数，对应 pull consumer 的 consumerNumber。

```csharp
[BufferPushCustomer(
    topicName: "topic-foo2",
    groupName: "group-foo2",
    batchSize: 100,
    serviceLifetime: ServiceLifetime.Singleton,
    concurrency: 2)]
public class Foo2PushConsumer(ILogger<Foo2PushConsumer> logger) : IBufferAutoCommitPushConsumer<Foo>
{
    public Task ConsumeAsync(IEnumerable<Foo> buffer, CancellationToken cancellationToken)
    {
        foreach (var foo in buffer)
        {
            logger.LogInformation("Foo2PushConsumer.ConsumeAsync: {Foo}", foo);
        }

        return Task.CompletedTask;
    }
}
```

```csharp
[BufferPushCustomer(
    "topic-bar",
    "group-bar",
    100,
    ServiceLifetime.Scoped,
    2)]
public class BarPushConsumer(ILogger<BarPushConsumer> logger) : IBufferManualCommitPushConsumer<Bar>
{
    public async Task ConsumeAsync(IEnumerable<Bar> buffer, IBufferConsumerCommitter committer,
        CancellationToken cancellationToken)
    {
        foreach (var bar in buffer)
        {
            logger.LogInformation("BarPushConsumer.ConsumeAsync: {Bar}", bar);
        }

        var commitTask = committer.CommitAsync();
        if (!commitTask.IsCompletedSuccessfully)
        {
            await commitTask.AsTask();
        }
    }
}
```

Producer 示例：

通过 IBufferQueue 获取到指定的 Producer，然后调用 ProduceAsync 方法发送数据。

在 Memory 模式下，如果设置了 BoundedCapacity，当缓冲区满时，ProduceAsync 方法会丢弃数据并抛出 MemoryBufferQueueFullException。可以使用 TryProduceAsync 方法来检查数据是否成功发送。

```csharp
[ApiController]
[Route("/api/[controller]")]
public class TestController(IBufferQueue bufferQueue) : ControllerBase
{
    [HttpPost("foo1")]
    public async Task<IActionResult> PostFoo1([FromBody] Foo foo)
    {
        var producer = bufferQueue.GetProducer<Foo>("topic-foo1");
        await producer.ProduceAsync(foo);
        return Ok();
    }

    [HttpPost("foo2")]
    public async Task<IActionResult> PostFoo2([FromBody] Foo foo)
    {
        var producer = bufferQueue.GetProducer<Foo>("topic-foo2");
        await producer.ProduceAsync(foo);
        return Ok();
    }

    [HttpPost("bar")]
    public async Task<IActionResult> PostBar([FromBody] Bar bar)
    {
        var producer = bufferQueue.GetProducer<Bar>("topic-bar");
        await producer.ProduceAsync(bar);
        // TryProduceAsync 会返回一个布尔值，表示数据是否成功发送
        // bool success = await producer.TryProduceAsync(bar);
        return Ok();
    }
}
```

## 示例项目

可参考 [`samples/WebAPI`](samples/WebAPI/) 中可直接运行的 ASP.NET Core 示例，了解 BufferQueue 的注册、生产以及 pull/push 消费方式。
