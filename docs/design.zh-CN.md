# BufferQueue 设计文档

[English 设计文档](README.md) | [简体中文设计文档](README.zh-CN.md)

此旧入口为已有链接保留。设计内容现已拆分为聚焦的文章；下方原有章节锚点会链接到对应文章。

## 目标

参见[架构与注册](design/architecture.zh-CN.md)。

## 公共模型

参见[架构与注册](design/architecture.zh-CN.md)。

## 总体架构

参见[架构与注册](design/architecture.zh-CN.md)。

## 内部 Partition 抽象

参见[架构与注册](design/architecture.zh-CN.md)。

## Partition 和 Consumer Group

参见[Partition 与并发](design/partitioning-and-concurrency.zh-CN.md)。

## Pull Consumer 设计

参见[Consumer 模型与投递](design/consumer-model.zh-CN.md)。

## Consumer 唤醒设计

参见[Consumer 模型与投递](design/consumer-model.zh-CN.md)。

## Memory 模式

参见[Memory 存储](design/memory.zh-CN.md)。

### 存储结构

参见[Memory 存储](design/memory.zh-CN.md#存储结构)。

### 写入

参见[Memory 存储](design/memory.zh-CN.md#写入路径)。

### 读取和提交

参见[Memory 存储](design/memory.zh-CN.md#读取与提交)。

### Segment 复用

参见[Memory 存储](design/memory.zh-CN.md#segment-复用)。

### 容量控制

参见[Memory 存储](design/memory.zh-CN.md#有界容量)。

## MemoryMappedFile 模式

参见[MemoryMappedFile 存储](design/memory-mapped-file.zh-CN.md)。

### 目录结构

参见[MemoryMappedFile 存储](design/memory-mapped-file.zh-CN.md#目录结构)。

### 记录格式

参见[MemoryMappedFile 存储](design/memory-mapped-file.zh-CN.md#记录格式)。

### 序列化

参见[MemoryMappedFile 存储](design/memory-mapped-file.zh-CN.md#序列化与-schema-兼容性)。

### Flush 策略

参见[MemoryMappedFile 存储](design/memory-mapped-file.zh-CN.md#flush-边界与写入)。

### 写入

参见[MemoryMappedFile 存储](design/memory-mapped-file.zh-CN.md#flush-边界与写入)。

### 恢复

参见[MemoryMappedFile 存储](design/memory-mapped-file.zh-CN.md#恢复)。

### Offset 持久化

参见[MemoryMappedFile 存储](design/memory-mapped-file.zh-CN.md#consumer-checkpoint)。

### Segment 保留策略

参见[MemoryMappedFile 存储](design/memory-mapped-file.zh-CN.md#segment-保留策略)。

### Producer Offset 持久化

参见[MemoryMappedFile 存储](design/memory-mapped-file.zh-CN.md#producer-checkpoint)。

## Push Consumer 模式

参见[Consumer 模型与投递](design/consumer-model.zh-CN.md)。

## 依赖注入

参见[架构与注册](design/architecture.zh-CN.md)。

## 并发模型

参见[Partition 与并发](design/partitioning-and-concurrency.zh-CN.md)。

## 投递语义

参见[Consumer 模型与投递](design/consumer-model.zh-CN.md)。

## 扩展点

参见[架构与注册](design/architecture.zh-CN.md)。

## 已知限制

参见[MemoryMappedFile 存储](design/memory-mapped-file.zh-CN.md)和
[Memory 存储](design/memory.zh-CN.md)。

## 测试策略

参见[架构与注册](design/architecture.zh-CN.md)。
