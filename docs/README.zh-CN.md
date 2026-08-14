# BufferQueue 设计文档

[English](README.md) | [简体中文](README.zh-CN.md)

这些文章记录 BufferQueue 中不容易仅从公共 API 看出的实现取舍，面向需要评估、维护或扩展队列行为的
开发者。内容覆盖队列模型、投递语义、存储边界和持久化保证。

日常接入请先阅读[仓库总览](../README.zh-CN.md)中的安装和可运行示例。下面的文章描述当前仓库实现，
用于补充各 NuGet 包 README 中的使用说明。

## 文章

| 文章 | 重点 |
| --- | --- |
| [架构与注册](design/architecture.zh-CN.md) | 公共模型、项目边界、共享队列抽象、依赖注入、扩展点和测试覆盖。 |
| [Consumer 模型与投递](design/consumer-model.zh-CN.md) | Pull 和 push consumer、异步唤醒、提交、作用域和 at-least-once 行为。 |
| [Partition 与并发](design/partitioning-and-concurrency.zh-CN.md) | Consumer group、partition 分配、key 路由、顺序保证和单进程并发规则。 |
| [Memory 存储](design/memory.zh-CN.md) | 分段内存存储、写入和读取路径、segment 复用与有界容量。 |
| [MemoryMappedFile 存储](design/memory-mapped-file.zh-CN.md) | 持久化 segment 布局、序列化、flush 边界、恢复、checkpoint 和保留策略。 |

## 阅读顺序

先阅读[架构与注册](design/architecture.zh-CN.md)，了解两种存储实现共享的边界。修改通用队列行为前，
继续阅读 [Consumer 模型与投递](design/consumer-model.zh-CN.md) 和
[Partition 与并发](design/partitioning-and-concurrency.zh-CN.md)。随后根据所修改的实现阅读对应存储文章：
[Memory](design/memory.zh-CN.md) 或
[MemoryMappedFile](design/memory-mapped-file.zh-CN.md)。

## 范围

BufferQueue 支持单个进程内的并发生产和消费。MemoryMappedFile 是面向一个 active queue 实例的本地
持久化存储，不协调多个进程之间的写入。持久化 topic 在重启前后必须保持兼容的 partition 路由和
serializer schema。

原有的单文件入口仍保留在 [docs/design.zh-CN.md](design.zh-CN.md)，作为向后兼容的索引。
