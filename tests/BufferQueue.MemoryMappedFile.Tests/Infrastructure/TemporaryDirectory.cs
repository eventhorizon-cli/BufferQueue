// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Runtime.CompilerServices;

namespace BufferQueue.MemoryMappedFile.Tests.Infrastructure;

internal sealed class TemporaryDirectory : IDisposable
{
    private const int DeleteAttempts = 3;
    private readonly object _disposeLock = new();
    private bool _disposed;

    public TemporaryDirectory([CallerMemberName] string? testName = null)
    {
        Path = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            "BufferQueue.Tests",
            testName ?? "UnknownTest",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path);
    }

    public string Path { get; }

    public void Dispose()
    {
        lock (_disposeLock)
        {
            if (_disposed)
            {
                return;
            }

            for (var attempt = 1; attempt <= DeleteAttempts; attempt++)
            {
                try
                {
                    Directory.Delete(Path, recursive: true);
                    _disposed = true;
                    return;
                }
                catch (DirectoryNotFoundException)
                {
                    _disposed = true;
                    return;
                }
                catch (Exception exception) when (
                    exception is IOException or UnauthorizedAccessException && attempt < DeleteAttempts)
                {
                    Thread.Sleep(TimeSpan.FromMilliseconds(50 * attempt));
                }
            }
        }
    }
}
