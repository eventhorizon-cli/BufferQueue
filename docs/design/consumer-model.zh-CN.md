# Consumer 模型与投递

[English](consumer-model.md) | [简体中文](consumer-model.zh-CN.md)

[设计文档首页](../README.zh-CN.md)

## Pull Consumer

`BufferPullConsumer<TItem>` 是唯一的通用 pull consumer 实现，不依赖具体存储。它负责：

- 保存已分配的 partition 列表；
- 按 round-robin 选择 partition；
- 从 partition 拉取 batch；
- 当前 partition 无数据时尝试其他已分配 partition；
- 所有已分配 partition 都无数据时异步等待；
- 提交手动消费的 batch；
- 在 `AutoCommit` 开启时自动提交。

消费接口返回 batch 的异步流：

~~~csharp
await foreach (var batch in consumer.ConsumeAsync(cancellationToken))
{
    foreach (var item in batch)
    {
        // Process item.
    }

    await consumer.CommitAsync();
}
~~~

当 `AutoCommit` 为 false 时，consumer 的读取位置不会在调用
`CommitAsync` 前推进。这在当前进程内提供 at-least-once 语义。对 MemoryMappedFile 模式而言，
已经到达 flush 边界的记录也会在进程重启后保留该语义；`CommitAsync` 本身会强制建立该边界。

group、consumer、partition、顺序和分配之间的关系参见
[Partition 与并发](partitioning-and-concurrency.zh-CN.md)。

## 等待与唤醒

Consumer 在无数据时不会自旋。通用 consumer 通过
`PendingDataValueTaskSource<T>` 等待：

1. Consumer 尝试从选中的 partition 拉取数据。
2. 如果没有数据，再尝试所有其他已分配 partition。
3. 如果仍然没有数据，重置 pending-data value-task source 并进入等待。
4. Producer 向某个 partition 追加数据。
5. Partition 通过 `IBufferPartitionConsumer<TItem>` 通知已注册 consumer。
6. Consumer 增加 pending-data version，并完成 pending value task。
7. Consumer 被唤醒后，从触发通知的 partition 尝试拉取数据。

pending-data version 用来避免 lost wake-up：如果数据在最后一次拉取尝试和进入等待状态之间到达，consumer
可以检测版本变化并重新尝试消费。

## Push Consumer

Push consumer 模式构建在 pull consumer 之上。Host service 扫描带 attribute 的 push consumer，
创建对应 pull consumer，并把 batch 交给 push consumer 实现处理。

Auto-commit push consumer 会在成功 pull 后、应用代码处理 batch 前推进进度，因此 handler failure 不会让该
batch 再次进入可重放状态。Manual-commit push consumer 会收到
`IBufferConsumerCommitter`，由业务代码决定何时提交；未提交的 batch 可能再次交付。

配置的 `ServiceLifetime` 决定 push consumer 的解析方式：

- `Singleton` consumer 从根容器解析，并在 batch 和并发 consumer loop 之间复用，因此必须线程安全。
- `Scoped` 和 `Transient` consumer 会为每个交付 batch 创建新的异步 DI scope。
  Handler 成功或抛出异常后，该 scope 及其捕获的服务会被异步释放；scoped dependency 不能逃逸出本次
  handler 调用。

## 投递语义

手动提交提供 at-least-once 投递：

- 已读取但未提交的 batch 可能再次交付。
- Auto commit 在成功 pull 后立即推进进度。
- Manual commit 在应用代码调用 `CommitAsync` 后推进进度。

Memory 的 offset 只在进程生命周期内存在。MemoryMappedFile 会把 producer offset 和已提交 consumer
offset 持久化到磁盘。Consumer commit 会先强制待处理 MMF 日志数据到达 flush 边界。在 Batch 模式下，
异常终止后未提交、未满一批的尾部 batch 不保证仍然存在。

存储特定行为参见 [Memory 存储](memory.zh-CN.md) 和
[MemoryMappedFile 存储](memory-mapped-file.zh-CN.md)。
