# Package releases

[简体中文](releases.zh-CN.md)

Package versions come from release tags, not project files.

| Tag | Published packages |
| --- | --- |
| `core-v<semver>` | `BufferQueue` only |
| `mmf-v<semver>` | `BufferQueue.MemoryMappedFile` only |
| `v<semver>` | Both packages at the same version; retained for joint releases |

**Release tags must point to a commit already on `main`.** Tags on unmerged branches are rejected before building or
publishing. A historical commit on `main` is allowed; the tag need not point to its latest commit. GitHub tag events
have no branch restriction, so the workflow fetches `main` and explicitly checks commit ancestry.

The version suffix must be SemVer, optionally with a prerelease suffix, without build metadata. For example,
`core-v2.1.1` selects only `BufferQueue`; `mmf-v2.1.2` selects only `BufferQueue.MemoryMappedFile`.
The aliases identify packages: `core` includes shared queue,
consumer, routing, and DI APIs as well as Memory storage.

## MMF dependencies

The MMF package depends on `BufferQueue`; independent package versions do not imply independent source compatibility.
For `mmf-v<semver>`, CI resolves the closest ancestor Core release tag (`core-v<semver>` or legacy `v<semver>`) and uses
that version for the Core dependency. That Core package must already be downloadable from NuGet. Core sources under
`src/BufferQueue/` must match that release, and a stable MMF release cannot depend on a prerelease Core package.

If Core sources have changed, publish Core first, then create the MMF tag, or use a joint `v<semver>` tag to publish both.
For a joint release, both package versions and the MMF dependency use the tag version. A Core-only tag never publishes
MMF automatically.

## Workflow

The publishing workflow validates the tag and `main` ancestry, restores, builds, and tests the whole solution on
.NET 8 and .NET 10, then packs only the selected packages. It verifies the selected package files, identities,
framework assemblies, package README, and MMF dependency versions before uploading artifacts and publishing to NuGet.
A shared concurrency group serializes publishing runs.

Versions are injected per project through an MSBuild props file generated under the runner's temporary directory.
No version numbers or generated version files are committed to the project. Publication uses only `NUGET_API_KEY`.
Updating this configuration does not create a tag or publish a package; publication starts when an authorized release
tag is pushed. A GitHub Release and its package assets should be completed as part of the same release operation.
