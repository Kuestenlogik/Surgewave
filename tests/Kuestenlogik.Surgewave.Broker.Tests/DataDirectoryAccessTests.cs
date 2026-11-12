using Kuestenlogik.Surgewave.Broker.Startup;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// A broker whose data directory it may not write to says so at its start, with the directory and the way to mend
/// it, instead of ending in the access-denied stack trace of the first component that writes (#276).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class DataDirectoryAccessTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("surgewave-data-access-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    [Fact]
    public void ADirectoryTheProcessMayWriteTo_IsNoProblem_AndIsLeftEmpty()
    {
        var directory = Path.Combine(_root, "data");

        Assert.Null(DataDirectoryAccess.Problem(directory));
        Assert.Empty(Directory.EnumerateFileSystemEntries(directory));
    }

    [Fact]
    public void WhatAnEarlierStartLeftInTheDirectory_IsLeftAsItIs()
    {
        var directory = Path.Combine(_root, "data");
        Directory.CreateDirectory(Path.Combine(directory, "orders", "partition-0"));
        var file = Path.Combine(directory, "meta.properties");
        File.WriteAllText(file, "version=1");
        var written = File.GetLastWriteTimeUtc(file);

        Assert.Null(DataDirectoryAccess.Problem(directory));
        Assert.Equal(["meta.properties", "orders"], Directory.EnumerateFileSystemEntries(directory).Select(Path.GetFileName).Order());
        Assert.Empty(Directory.EnumerateFiles(Path.Combine(directory, "orders")));
        Assert.Equal("version=1", File.ReadAllText(file));
        Assert.Equal(written, File.GetLastWriteTimeUtc(file));
    }

    [Fact]
    public void AFileInTheDirectoryThatCannotBeWritten_IsAProblem_ThoughTheDirectoryItselfCanBe()
    {
        // What a volume looks like that Docker created with the image's owner and a broker running as root filled.
        var directory = Path.Combine(_root, "data");
        Directory.CreateDirectory(directory);
        var file = Path.Combine(directory, "meta.properties");
        File.WriteAllText(file, "version=1");
        File.SetAttributes(file, FileAttributes.ReadOnly);
        try
        {
            var problem = DataDirectoryAccess.Problem(directory);

            Assert.NotNull(problem);
            Assert.Contains("meta.properties", problem, StringComparison.Ordinal);
        }
        finally
        {
            File.SetAttributes(file, FileAttributes.Normal);
        }
    }

    [Fact]
    public void ADirectoryThatCannotBeWrittenTo_IsNamedWithTheWayToMendIt()
    {
        // A file where the directory is to be: nothing can be created in it, whoever asks.
        var blocked = Path.Combine(_root, "taken");
        File.WriteAllText(blocked, "not a directory");
        var directory = Path.Combine(blocked, "data");

        var problem = DataDirectoryAccess.Problem(directory);

        Assert.NotNull(problem);
        Assert.Contains(directory, problem, StringComparison.Ordinal);
        Assert.Contains("Surgewave:DataDirectory", problem, StringComparison.Ordinal);
    }
}
