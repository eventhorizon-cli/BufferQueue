# Partition 与并发

[English](partitioning-and-concurrency.md) | [简体中文](partitioning-and-concurrency.zh-CN.md)

[设计文档首页](../README.zh-CN.md)

## Partition 与 Consumer Group

每个 topic 可以包含一个或多个 partition。Producer 默认使用 round-robin 分发。Memory 和
MemoryMappedFile topic 都可以通过带 key-selector delegate 的 `UsePartitionKey`，
把相同 key 路由到同一个 partition。Selector 必须保持确定性，并能安全地被并发调用。

Consumer 按 consumer group 创建。一个 group 可以包含多个 consumer，partition 会在组内均分：

- Consumer 数量必须大于零。
- Consumer 数量不能超过 partition 数量。
- 每个 group 有独立的读取进度。
- 每个 group 都会消费 topic 的全部消息，但组内通过 partition 分配实现负载均衡。

例如，五个 partition 和两个 consumer：

~~~text
consumer-0: partition-0, partition-1, partition-2
consumer-1: partition-3, partition-4
~~~

不同 group 相互独立。两个 group 消费同一个 topic 时各自维护进度。Group 创建后 consumer 数量固定；
同一个 queue 实例中重复创建相同 group name 会被拒绝。

Memory consumer group 可以与生产并发创建，已有 group 的分配保持不变。生产继续进行时创建的 group 从各
partition 当前仍可读的最早位置开始；不存在 topic 全局原子的注册时刻，也不保证该 group 能读取全部历史记录。

顺序只在单个 partition 内得到保证，不保证跨 partition 的全局顺序。同一 group 中不同 consumer 获得
不同 partition 分配，因此不会有两个组成员竞争消费同一个 partition。

## PartitionKey 路由

Round-robin 路由将追加操作分散到不同 partition。启用 PartitionKey 路由后：

- 数值 selector 的结果必须是有限整数，并使用 `(key - 1)` 对
  `PartitionNumber` 的归一化数学取模映射；零和负数也可以作为 key。
- 字符串 selector 对完整 key 使用种子为零的 MurmurHash3 x86_32，不使用 `string.GetHashCode()`。
  每个 UTF-16 码元依次贡献低字节和高字节，包括代理码元及内嵌空字符；不执行规范化或大小写转换，
  也不添加 BOM 或终止符。每两个码元组成一个小端 32 位块；末尾单个码元作为两字节尾部处理。
  按标准算法混入字节长度并执行 fmix32，运算按模 2^32 回绕。无符号哈希对 `PartitionNumber`
  取模得到 partition。空字符串合法，null 被拒绝；单 partition 在参数校验后始终选择零。

字符串路由不分配内存，时间复杂度为 O(key 长度)。固定字节序和常量使映射不受进程哈希随机化、
机器字节序或目标框架影响。实现遵循 [MurmurHash3 x86_32 参考实现](https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp)，
直接处理成对的 UTF-16 码元，不编码成临时字节数组。
这将直接替换原来的四码元映射，不提供旧模式。升级会改变字符串 partition 分配；已有 MMF 日志仍可读取，
但同 key 的新消息可能进入其他 partition，因此不保证跨升级的按 key 顺序。

相同 key 因此能保持 partition 内顺序，不同 key 仍可能映射到同一个 partition。

对于持久化的 MemoryMappedFile topic，重启前后不能改变 selector、PartitionKey 路由行为或
`PartitionNumber`。调小已有 MMF topic 的 partition count 时，如果已有 partition directory
超过当前配置，启动也会失败。恢复规则参见
[MemoryMappedFile 存储](memory-mapped-file.zh-CN.md)。

## 批量路由

`ProduceAsync` 和 `TryProduceAsync` 的批量重载仍按单条数据进行路由。一个批次不会因为整体提交，
就被固定分配到某一个 partition：

- Round-robin 路由在处理批次中的每条数据时都会轮转一次。
- PartitionKey 路由会为每条数据调用 selector；相同 key 的数据会在选定 partition 内保持输入顺序。

同一批次中被路由到不同 partition 的数据不具备全局顺序保证。Queue 仍然只保证每个 partition 内的顺序。

有界 Memory topic 处于 `Wait` 模式时，超过配置容量的输入批次会被切成连续的容量大小切片。每个切片仍按
单条数据路由，因此 round-robin 会跨切片继续轮转；相同 PartitionKey 在其选定 partition 内仍保持输入顺序。

## 单进程并发

Queue 的设计目标是在一个进程内并发生产和消费：

- Producer 默认通过 round-robin counter 选择 partition，或通过配置的 PartitionKey selector 选择。
  Selector 实现必须能安全地被并发调用。
- Memory 模式中，默认 round-robin 的 partition 选择和批量范围预留使用短时的 topic 级协调锁。每个已接纳
  的批量或批量切片都会先一次性取得完整容量，再保留选择范围，并在每个 partition 的 append lock 内追加
  对应切片。存在活跃批量 append 时，单条 round-robin 写入也会取得目标 partition 的锁；Consumer 的状态
  变更使用同一个协调锁。PartitionKey 路由先选择 partition，再获取该 partition 的 append lock。同一
  partition 的 append 仍然串行。
- Memory partition 只会在写入 item 后发布可读 segment cursor，因此 consumer 不会读到未写入 slot，
  读取时也不需要获取 append lock。
- Consumer group 的创建受 queue-level lock 保护。
- Memory consumer 的注册和注销由现有的 partition append lock 串行化。注册时先从当前 `HashSet` 构建通知集合，
  初始化 reader 后，再通过 volatile 写入发布集合。已发布的集合不会再修改。Producer 通过 volatile 读取集合并
  在锁外遍历，因此生产路径不会复制集合，也不会为每条数据新增通知锁。在途通知可能仍使用旧集合，并通知刚刚
  注销的 consumer；注销不会等待这类通知完成。新 consumer 会先读取已有数据再进入等待，因此没有出现在旧通知
  集合中本身不会导致丢失唤醒。
- Consumer 等待和唤醒状态受 `ReaderWriterLockSlim` 保护。
- MemoryMappedFile 的 producer 和 consumer checkpoint 使用 replace-or-move 语义，读取方不会看到
  部分写入的 offset 文件。

MemoryMappedFile 不提供针对同一 topic directory 的跨进程写入协调。除非加入外部协调，否则应把它视为
一个 active queue 实例使用的本地持久化机制。

## 相关文档

[Consumer 模型与投递](consumer-model.zh-CN.md)介绍 batch pull、等待、提交和投递。
[Memory 存储](memory.zh-CN.md)和 [MemoryMappedFile 存储](memory-mapped-file.zh-CN.md)说明
选定 partition 后的存储行为。
