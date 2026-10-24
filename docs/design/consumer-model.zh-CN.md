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

Consumer 按环形顺序扫描已分配的 partition。成功拉取后，下次扫描从实际提供 batch 的
partition 之后开始；全空扫描不推进游标。持续可读的 partition 每轮各获得一次拉取机会，
即使其余 partition 为空也如此。这是 batch 调度公平性，不保证处理时间相同、按 key 公平或全局顺序。
手动提交仍作用于最近交付的 batch；未提交 partition 再次轮到时可以重放其 batch。
游标始终保持在已分配数组的范围内。

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

`ConsumeAsync` 返回 `IAsyncEnumerable<IReadOnlyList<TItem>>`。每个 batch 都是已生成且独立持有数据的快照。
`IReadOnlyList<TItem>` 提供 `Count` 和索引访问，同时通过 `IEnumerable<TItem>` 继续支持 `foreach` 和 LINQ。
这个 list 接口不会让 item 本身变成不可变；batch 在后续 pull、commit 或 segment 复用后仍然有效。
这个公开签名的变更不兼容旧二进制，使用这些接口的应用和库都需要重新编译。普通枚举代码通常不需要修改；
consumer 接口实现、mock 和 push handler 需要更新参数或返回类型。

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
7. Consumer 被唤醒后继续同一环形扫描；通知仅用于唤醒，不改变 partition 的优先级。

pending-data version 用于避免最后一次空扫描与等待之间丢失唤醒。Consumer 在注册锁内先通过
interlocked exchange 发布等待标志，再检查版本。版本变化时清除标志并重新扫描；版本未变时，
在释放锁前重置 pending value-task source。

通知先增加版本，再检查等待标志。没有已发布的等待者时直接返回，不获取锁；否则取得注册锁，
再次检查标志，最多完成一次等待。Interlocked 操作保证顺序：注册方要么看到新版本，通知方要么看到
已发布的等待者。标志可能先于等待源重置而可见，因此完成等待仍必须在同一把锁内执行。
通知仍只用于唤醒 round-robin 扫描。

## Push Consumer

Push consumer 模式构建在 pull consumer 之上。Host service 扫描带 attribute 的 push consumer，
创建对应 pull consumer，并把 batch 交给 push consumer 实现处理。

Push consumer handler 同样接收 `IReadOnlyList<TItem>`，可以直接使用 `Count` 和索引，也继续支持枚举。
Handler 可以在异步处理期间保留 batch；Auto Commit 不代表 handler 已经处理完成，因此 queue 不会自动回收或使已交付的
batch 失效。

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
