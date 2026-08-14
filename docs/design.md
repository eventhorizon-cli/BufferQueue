# BufferQueue Design

[English design index](README.md) | [Simplified Chinese design index](README.zh-CN.md)

This legacy entry point remains available for existing links. The design material now lives in
focused articles; the original section anchors below lead to the corresponding article.

## Purpose

See [Architecture and registrations](design/architecture.md).

## Public Model

See [Architecture and registrations](design/architecture.md).

## High-Level Architecture

See [Architecture and registrations](design/architecture.md).

## Internal Partition Abstraction

See [Architecture and registrations](design/architecture.md).

## Partitioning and Consumer Groups

See [Partitioning and concurrency](design/partitioning-and-concurrency.md).

## Pull Consumer Design

See [Consumer model and delivery](design/consumer-model.md).

## Consumer Wake-Up Design

See [Consumer model and delivery](design/consumer-model.md).

## Memory Mode

See [Memory storage](design/memory.md).

### Storage Layout

See [Memory storage](design/memory.md#storage-layout).

### Append

See [Memory storage](design/memory.md#append-path).

### Read and Commit

See [Memory storage](design/memory.md#read-and-commit).

### Segment Recycling

See [Memory storage](design/memory.md#segment-recycling).

### Capacity

See [Memory storage](design/memory.md#bounded-capacity).

## MemoryMappedFile Mode

See [MemoryMappedFile storage](design/memory-mapped-file.md).

### Directory Layout

See [MemoryMappedFile storage](design/memory-mapped-file.md#directory-layout).

### Record Format

See [MemoryMappedFile storage](design/memory-mapped-file.md#record-format).

### Serialization

See [MemoryMappedFile storage](design/memory-mapped-file.md#serialization-and-schema-compatibility).

### Flush Strategies

See [MemoryMappedFile storage](design/memory-mapped-file.md#flush-boundaries-and-append).

### Append

See [MemoryMappedFile storage](design/memory-mapped-file.md#flush-boundaries-and-append).

### Recovery

See [MemoryMappedFile storage](design/memory-mapped-file.md#recovery).

### Offset Persistence

See [MemoryMappedFile storage](design/memory-mapped-file.md#consumer-checkpoints).

### Segment Retention

See [MemoryMappedFile storage](design/memory-mapped-file.md#segment-retention).

### Producer Offset Persistence

See [MemoryMappedFile storage](design/memory-mapped-file.md#producer-checkpoint).

## Push Consumer Mode

See [Consumer model and delivery](design/consumer-model.md).

## Dependency Injection

See [Architecture and registrations](design/architecture.md).

## Concurrency Model

See [Partitioning and concurrency](design/partitioning-and-concurrency.md).

## Delivery Semantics

See [Consumer model and delivery](design/consumer-model.md).

## Extension Points

See [Architecture and registrations](design/architecture.md).

## Known Limitations

See [MemoryMappedFile storage](design/memory-mapped-file.md) and
[Memory storage](design/memory.md).

## Testing Strategy

See [Architecture and registrations](design/architecture.md).
