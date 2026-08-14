# BufferQueue Design Notes

[English](README.md) | [Simplified Chinese](README.zh-CN.md)

These notes describe the implementation decisions behind BufferQueue. They are intended for
maintainers and users who need to understand the queue model, delivery behavior, storage
boundaries, and persistence guarantees beyond the public API examples.

Start with the [repository overview](../README.md) for package installation and runnable usage.
The articles below describe the checked-in implementation and complement the package READMEs.

## Articles

| Article | Focus |
| --- | --- |
| [Architecture and registrations](design/architecture.md) | Public model, project boundaries, shared queue abstractions, dependency injection, extension points, and test coverage. |
| [Consumer model and delivery](design/consumer-model.md) | Pull and push consumers, asynchronous wake-up, commits, scopes, and at-least-once behavior. |
| [Partitioning and concurrency](design/partitioning-and-concurrency.md) | Consumer groups, partition assignment, key routing, ordering, and in-process concurrency rules. |
| [Memory storage](design/memory.md) | Segmented in-memory storage, append and read paths, recycling, and bounded capacity. |
| [MemoryMappedFile storage](design/memory-mapped-file.md) | Durable segment layout, serialization, flush boundaries, recovery, checkpoints, and retention. |

## Reading order

Read [architecture and registrations](design/architecture.md) first to understand what is shared
between storage implementations. Continue with the [consumer model](design/consumer-model.md) and
[partitioning and concurrency](design/partitioning-and-concurrency.md) before changing shared
queue behavior. Read the storage article that matches the implementation being changed:
[Memory](design/memory.md) or [MemoryMappedFile](design/memory-mapped-file.md).

## Scope

BufferQueue supports concurrent production and consumption within one process. MemoryMappedFile
is local durable storage for one active queue instance; it does not coordinate writers across
processes. Persisted topics require compatible partition routing and serializer schemas across
restarts.

The previous monolithic entry point remains available at [docs/design.md](design.md) as a
backward-compatible index.
