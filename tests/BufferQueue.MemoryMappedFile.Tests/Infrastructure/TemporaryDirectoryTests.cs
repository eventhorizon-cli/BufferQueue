// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

namespace BufferQueue.MemoryMappedFile.Tests.Infrastructure;

public class TemporaryDirectoryTests
{
    [Fact]
    public void Dispose_Deletes_Directory_And_Is_Idempotent()
    {
        var temporaryDirectory = new TemporaryDirectory();
        var path = temporaryDirectory.Path;
        File.WriteAllText(System.IO.Path.Combine(path, "test.txt"), "test");

        temporaryDirectory.Dispose();
        temporaryDirectory.Dispose();

        Assert.False(Directory.Exists(path));
    }

    [Fact]
    public void Dispose_Ignores_Missing_Directory()
    {
        var temporaryDirectory = new TemporaryDirectory();
        Directory.Delete(temporaryDirectory.Path);

        temporaryDirectory.Dispose();
    }
}
