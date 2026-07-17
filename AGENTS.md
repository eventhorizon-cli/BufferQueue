# AGENTS.md

This file gives coding agents the project-specific context needed to work safely in this repository.

## Project Overview

BufferQueue is a .NET library for typed, topic-based buffering with batch consumption. It currently supports:

- Memory-backed queues.
- Mapped-file-backed queues.
- Pull consumers.
- Push consumers built on top of pull consumers.
- Auto-commit and manual-commit consumption.
- Multiple consumer groups per topic.
- Multiple consumers per group with partition-based load balancing.

The target framework for the main library and tests is `net8.0`.

## Repository Layout

```text
src/BufferQueue/                 Main library
src/BufferQueue/Memory/          In-memory storage implementation
src/BufferQueue.MemoryMappedFile/      Memory-mapped-file storage implementation
src/BufferQueue.MemoryMappedFile/Serializers/  MMF serialization contracts and implementations
src/BufferQueue/PushConsumer/    Push consumer integration
tests/BufferQueue.Tests/         Core and in-memory xUnit tests
tests/BufferQueue.MemoryMappedFile.Tests/  Memory-mapped-file xUnit tests
tests/BufferQueue.Benchmarks/    BenchmarkDotNet benchmarks
samples/WebAPI/                  Sample ASP.NET Core app
docs/                            Design docs and assets
```

Important docs:

- `README.md`
- `README.zh-CN.md`
- `docs/design.md`
- `docs/design.zh-CN.md`

## Common Commands

Run the full test suite:

```shell
dotnet test BufferQueue.sln
```

Verify formatting:

```shell
dotnet format BufferQueue.sln --verify-no-changes
```

Run benchmarks:

```shell
dotnet run -c Release --project tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj
```

## Architecture Notes

The public entry point is `IBufferQueue`. It resolves typed topic queues registered as keyed `IBufferQueue<T>` services.

The MemoryMappedFile implementation is an optional, separate assembly. `BufferQueue.MemoryMappedFile` depends on `BufferQueue` and reuses its shared internal queue abstractions through friend-assembly access. The core `BufferQueue` project must not reference `BufferQueue.MemoryMappedFile`. Keep the existing public namespaces and `.UseMemoryMappedFile(...)` registration API when changing this boundary.

The shared typed queue behavior is implemented by the internal abstract `BufferQueue<TItem>` base class. Storage-specific queues inherit it:

- `MemoryBufferQueue<T>`
- `MemoryMappedFileBufferQueue<T>`

Common pull consumer behavior is implemented once in `BufferPullConsumer<TItem>`.

Storage-specific behavior belongs behind `IBufferPartition<TItem>`:

- `MemoryBufferPartition<T>`
- `MemoryMappedFileBufferPartition<T>`

Keep upper-level queue behavior out of storage implementations unless the behavior is truly storage-specific.

## Storage Implementation Rules

### Memory

Memory mode stores items in linked memory segments and supports optional bounded capacity through `MemoryBufferQueueOptions.BoundedCapacity`.

Be careful with segment recycling. A segment can only be reused after all consumer groups have advanced past it.

### MemoryMappedFile

MemoryMappedFile mode stores serialized records in per-partition memory-mapped segment files.

Record format:

```text
4 bytes  payload length, little-endian int32
N bytes  payload
1 byte   record end marker
```

Segment end marker:

```text
int32 length == -1
```

MemoryMappedFile mode persists:

- producer offset in `producer.offset`;
- earliest retained segment boundary in `earliest.offset`;
- consumer offsets under `offsets/{escaped-group-name}/consumer.offset`.

Offset reads and writes should go through `OffsetCheckpoint`. Do not duplicate offset file IO in partition code.

When segment retention is enabled, reclaim only complete segments below the minimum committed offset of every known consumer group. Advance `earliest.offset` before disposing mappings and deleting files. Recovery and reads must never recreate a missing segment inside the retained range; fail fast instead.

Group directory names should stay readable when possible. Only characters that cannot be used in one path component are escaped.

MMF serializers belong under `src/BufferQueue.MemoryMappedFile/Serializers/` while retaining the public `BufferQueue.MemoryMappedFile` namespace. `UnmanagedMemoryMappedFileSerializer<T>` persists the native in-memory representation of an unmanaged value, so its layout, packing, endianness, runtime, and process architecture are part of the persisted schema.

MemoryMappedFile queues own their partitions, and partitions own all mapped files and view accessors. Preserve deterministic disposal, including constructor failure paths. DI registrations must let the container own MMF queue instances so provider disposal closes every mapping.

## Concurrency and Semantics

The queue is designed for concurrent production and consumption inside one process.

Expected delivery semantics:

- manual commit: at-least-once;
- auto commit: progress advances after a successful pull;
- uncommitted batches may be delivered again.

MemoryMappedFile mode currently persists offsets but does not provide multi-process writer coordination.

## Coding Guidelines

- Prefer existing abstractions and local style over new patterns.
- Keep common queue/consumer logic in shared code.
- Keep storage-specific details inside the relevant storage folder.
- Do not add public API unless the feature requires it.
- Keep comments short and only where they clarify non-obvious behavior.
- Preserve nullable annotations.
- Avoid unrelated refactors.
- Do not change README benchmark claims unless the benchmarks were rerun.
- Do not invent silent fallback behavior for corrupted or inconsistent state. If a checkpoint, offset, persisted log, or recovery invariant is invalid, fail fast with a clear exception instead of resetting progress or scanning from a guessed location.

## Testing Guidelines

Add or update tests for behavior changes.

Use focused tests for:

- commit and replay behavior;
- consumer wait and wake-up behavior;
- partition assignment;
- memory-mapped-file recovery;
- producer and consumer offset persistence;
- invalid or missing checkpoint files.

MMF tests must declare `using var temporaryDirectory = new TemporaryDirectory();` before queues or partitions. C# then disposes mappings before recursively deleting the test directory. Cleanup failures must remain visible rather than being silently ignored.

Before finishing a code change, run:

```shell
dotnet format BufferQueue.sln --verify-no-changes
dotnet test BufferQueue.sln
```

## Documentation Guidelines

Update both English and Chinese docs when changing user-visible behavior or storage layout:

- `docs/design.md`
- `docs/design.zh-CN.md`

If README usage changes, update both:

- `README.md`
- `README.zh-CN.md`

## Git and File Safety

The worktree may contain user changes. Do not revert unrelated changes. If a file already has changes, read it carefully and preserve existing work.

Avoid destructive commands such as `git reset --hard` or `git checkout --` unless explicitly requested.
