# 包发布

[English](releases.md)

包版本从发布 tag 解析，不写入项目文件。

| Tag | 发布的包 |
| --- | --- |
| `core-v<semver>` | 仅 `BufferQueue` |
| `mmf-v<semver>` | 仅 `BufferQueue.MemoryMappedFile` |
| `v<semver>` | 两个包使用相同版本，保留用于联合发布 |

**发布 tag 必须指向已经进入 `main` 的提交。** 未合并分支上的 tag 会在构建、发布前被拒绝。
允许指向 `main` 的历史提交，不要求必须是最新提交。GitHub 的 tag 事件不限制分支，因此工作流会拉取
`main` 并显式检查提交祖先关系。

版本后缀必须符合 SemVer，可带预发布后缀，不支持 build metadata。例如，`core-v2.1.1` 只选择
`BufferQueue`，`mmf-v2.1.2` 只选择 `BufferQueue.MemoryMappedFile`。
别名对应包：`core` 除了 Memory 存储，还包含共享的队列、Consumer、路由和 DI API。

## MMF 依赖

MMF 包依赖 `BufferQueue`；包版本独立不代表源码兼容性独立。对于 `mmf-v<semver>`，CI 会找到当前提交历史中
最近的 Core 发布 tag（`core-v<semver>` 或旧的 `v<semver>`），使用它的版本作为 Core 依赖。
该 Core 包必须已经能从 NuGet 下载，`src/BufferQueue/` 下的源码必须与对应发布一致，稳定版 MMF 不能依赖预发布版 Core。

如果 Core 源码已经变化，应先发布 Core，再创建 MMF tag；也可以用联合发布的 `v<semver>` 同时发布两个包。
联合发布时，两个包及 MMF 的 Core 依赖都使用 tag 中的版本。仅发布 Core 不会自动发布 MMF。

## 工作流

发布工作流先校验 tag 和 `main` 祖先关系，然后在 .NET 8 和 .NET 10 上还原、构建、测试整个解决方案，
只打包所选的包。上传产物和推送 NuGet 前，会验证所选包文件、包标识、目标框架程序集、包 README 和 MMF 依赖版本。
共享 concurrency group 保证发布工作流串行执行。

版本通过 runner 临时目录中生成的 MSBuild props 文件按项目注入，不向项目提交版本号或生成的版本文件。
发布仅使用 `NUGET_API_KEY`。更新这份配置不会创建 tag 或发布包；只有推送经过授权的发布 tag 才会触发发布。
同一次发布操作还应完成对应 GitHub Release 及包附件。
