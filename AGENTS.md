# AGENTS.md

You are an AI coding assistant for this repository.

## Scope and working style

- These instructions apply repository-wide unless a more specific `AGENTS.md` exists below the target path.
- Follow [`.editorconfig`](.editorconfig) and nearby code before introducing a new style or abstraction.
- Keep changes focused, maintainable, and production-ready. Preserve unrelated user changes.
- The projects define the supported target frameworks and language version. Do not change either policy incidentally.
- The library, sample, and test projects target `net8.0` and `net10.0`. Benchmarks intentionally target `net10.0`
  only.
- This repository uses the standard MIT License in `LICENSE`. Do not add file-level license headers.

## References

Use the design-note index as the source of truth for detailed, evolving guidance instead of duplicating it here:

- [Design-note index](docs/README.md): English architecture, consumer, routing, and storage notes.
- [Architecture](docs/design/architecture.md): package boundary, queue ownership, dependency injection, and extension
  points.
- [Consumer model](docs/design/consumer-model.md): consumer groups, pull and push consumers, wake-up behavior, and
  delivery semantics.
- [Partitioning and concurrency](docs/design/partitioning-and-concurrency.md): routing strategies, selector
  requirements, locking, and visibility rules.
- [Memory storage](docs/design/memory.md): segment layout, bounded capacity, recycling, and offsets.
- [Memory-mapped-file storage](docs/design/memory-mapped-file.md): record format, persistence, recovery, retention,
  serializers, and flush boundaries.
- The matching `*.zh-CN.md` note is the Simplified Chinese counterpart of each English design note.

Read the relevant note before changing an architectural boundary, storage behavior, partitioning rule, delivery
semantic, persistence format, or consumer lifecycle.

## Architecture constraints

- `IBufferQueue` is the public entry point. It resolves typed topic queues registered as keyed `IBufferQueue<T>`
  services and exposes producers for runtime-selected topics.
- `BufferQueue<TItem>` contains shared typed queue behavior. It owns consumer-option validation, consumer-group
  registration, partition assignment, pull-consumer creation, and the topic producer.
- `BufferPullConsumer<TItem>` is the common pull-consumer implementation. Do not create storage-specific copies of
  group assignment, batch consumption, commits, or waiting behavior.
- Storage-specific behavior belongs behind `IBufferPartition<TItem>`. Keep upper-level queue behavior out of storage
  implementations unless it is truly storage-specific.
- Memory storage belongs in `src/BufferQueue/Memory/`. MemoryMappedFile storage belongs in
  `src/BufferQueue.MemoryMappedFile/`.
- `BufferQueue.MemoryMappedFile` is an optional assembly that depends on `BufferQueue` and uses shared internal queue
  abstractions through friend-assembly access. The core `BufferQueue` project must not reference the MMF project.
  Preserve the current public namespaces and `.UseMemoryMappedFile(...)` registration API.
- A new storage implementation should supply its partitions, producer, queue type, options, and DI registration while
  reusing the common queue and consumer layers.
- Use keyed DI registrations for typed topics. Do not add a factory when keyed services already express the ownership
  and lifetime correctly.

## Storage and persistence constraints

### Memory

- Memory mode stores items in linked, fixed-size segments and can enforce a total bounded capacity through
  `MemoryBufferQueueOptions.BoundedCapacity`.
- A memory segment may be recycled only after every consumer group has advanced past it. A slow group must never lose
  unread data.
- `MemoryBufferQueueOptions.FullMode` defaults to `Wait`: `ProduceAsync` asynchronously waits with cancellation support
  when a bounded queue has no capacity. `Fail` throws `BufferQueueFullException` immediately. `TryProduceAsync` never
  waits for bounded capacity and returns `false` when admission is unavailable.
- Batch admission is all-or-nothing. In `Wait` mode, `ProduceAsync` waits for the complete batch, and a batch larger
  than the configured capacity is invalid. Capacity is released when the minimum committed position across all known
  consumer groups advances, including partial segments.

### MemoryMappedFile

- MMF records are `int32 little-endian payload length`, payload bytes, and one record-end marker byte. A segment-end
  marker is an `int32` length of `-1`.
- Go through `OffsetCheckpoint` for all `producer.offset`, `earliest.offset`, and consumer-offset reads and writes.
  Do not duplicate checkpoint file IO in partition code.
- When retention is enabled, reclaim only complete segments below the minimum committed offset of every known consumer
  group. Advance `earliest.offset` before disposing mappings and deleting old segment files.
- Recovery and reads must never recreate a missing segment inside the retained range. Invalid checkpoints, offsets,
  record boundaries, or retained files must fail fast with a clear exception instead of silently resetting progress.
- Group directories should remain readable whenever possible. Escape only characters that are unsafe in one path
  component.
- MMF serializers belong in `src/BufferQueue.MemoryMappedFile/Serializers/` while retaining the public
  `BufferQueue.MemoryMappedFile` namespace. Serializer compatibility is part of the persisted topic schema.
- An MMF queue owns its partitions, and partitions own their mappings and view accessors. Preserve deterministic
  disposal, including constructor failure paths. DI must own queue instances so provider disposal closes all mappings.

## Partitioning, concurrency, and delivery

- Producers use round-robin routing unless a topic configures `UsePartitionKey` with a selector delegate.
- A partition-key selector must be deterministic and safe for concurrent invocation. For MMF topics, the selector and
  partition count must remain stable across restarts when per-key ordering matters.
- Equal partition keys must select one partition; different keys may still collide. Do not use process-randomized
  `string.GetHashCode()` for persistent routing.
- Memory mode may serialize round-robin append selection. Partition-key routing may write different selected
  partitions in parallel, but appends to the same partition remain serialized.
- Publish a memory item only after its slot has been written. Consumers must not observe an unwritten slot.
- Manual commit is at-least-once: an uncommitted batch may be delivered again. Auto-commit advances progress after a
  successful pull. MMF commit must not advance a consumer checkpoint beyond data that has reached a flush boundary.
- MMF mode supports concurrent production and consumption only within one process. It does not provide multi-process
  writer coordination for the same topic directory.

## C# and dependencies

- Preserve nullable annotations, cancellation propagation, and async disposal behavior. Use `ConfigureAwait(false)`
  where it is consistent with nearby library code.
- Keep public APIs, XML documentation, exception behavior, and option validation compatible unless the feature
  explicitly requires a breaking change.
- Prefer constructor injection and standard Microsoft DI. Add an interface only for a real replacement, storage, or
  testing boundary; do not abstract options, data objects, framework types, or internal implementation details by
  default.
- Keep each type focused on one cohesive responsibility. Extract an internal collaborator only when it has independent
  ownership, not merely to split a file mechanically.
- Prefer base libraries and existing dependencies. Explain every new production dependency and its target-framework
  compatibility impact. Do not add a repository `NuGet.config` or hard-code an alternative package source.
- Write code comments and XML documentation in English. Keep comments short and use them for non-obvious invariants.

## Documentation

- Update documentation when behavior, configuration, public APIs, package workflows, storage layout, or architectural
  decisions change.
- Keep English and Simplified Chinese README and design-note pairs semantically synchronized. Preserve identifiers,
  code, commands, links, technical facts, and boundary conditions in both languages.
- Root READMEs are user guides. Put internal queue ownership, persistence layout, concurrency rules, and test design in
  the matching design note under `docs/design/`.
- Every functional change must update the affected NuGet package README:
  `docs/nuget/BufferQueue/README.md` and/or `docs/nuget/BufferQueue.MemoryMappedFile/README.md`.
- A shared queue or consumer behavior change generally requires both package READMEs. Core-only changes require the
  core package README; MMF-only changes require the MMF package README.
- Do not update documentation for a purely internal refactor with no user-visible or architectural consequence. State
  any documentation that could not be updated and why.

## Workflows and releases

- Keep GitHub Actions permissions minimal, use concurrency controls for cancelable CI, and cache NuGet packages by
  solution and project inputs.
- Build and test both supported target frameworks. Keep the two test projects independently visible in CI so core and
  MMF failures are easy to identify.
- Release tags use `v<semver>`. A publication workflow must validate the tag, build and test before packing, upload
  package artifacts, verify expected package files, and use only `NUGET_API_KEY` for NuGet publication.
- Preserve package metadata and package README inclusion when changing the packaging workflow.

## Testing and validation

- For meaningful public-API, storage, concurrency, persistence, or delivery-semantic changes: update the relevant
  bilingual design note, define the public API contract, add focused tests, then implement the behavior.
- Prefer the smallest focused test while iterating. Run it before implementation for a behavior change when a useful
  red phase exists. Documentation-only, workflow-only, mechanical configuration, and covered refactor work do not need
  an artificial failing test.
- Add or update tests for behavior changes. Focus on commits and replay, consumer waiting and wake-up, partition
  assignment, segment recycling, bounded capacity, MMF recovery, producer and consumer offset persistence, and invalid
  checkpoint files as applicable.
- MMF tests must declare `using var temporaryDirectory = new TemporaryDirectory();` before queues or partitions. This
  disposes mappings before recursive directory cleanup, and cleanup failures must remain visible.
- Keep benchmarks under `tests/BufferQueue.Benchmarks/`. Re-run them before changing benchmark claims and report the
  configuration and baseline used for a performance conclusion.

Run the narrowest relevant test project while iterating. Before finishing a C# change, run the affected complete test
project(s), then the repository checks from the root:

```bash
dotnet format BufferQueue.slnx --verify-no-changes
dotnet restore BufferQueue.slnx
dotnet build BufferQueue.slnx --configuration Release --no-restore
dotnet test BufferQueue.slnx --configuration Release --no-build --no-restore
```

For workflow and documentation-only changes, run the relevant syntax, link, and diff checks instead and clearly state
that no production behavior changed.

## Git and file safety

- The worktree may contain user changes. Read overlapping changes carefully, preserve them, and never revert unrelated
  work.
- Avoid destructive commands such as `git reset --hard` or `git checkout --` unless explicitly requested.
- Do not edit generated `bin/` or `obj/` content.
