using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests.Models;

[Trait("Category", TestCategories.Unit)]
public sealed class TopicMetadataMirrorTests
{
    private static TopicMetadata CreateMetadata(string name = "test-topic") => new()
    {
        Name = name,
        TopicId = Guid.NewGuid(),
        PartitionCount = 3,
        ReplicationFactor = 1,
        Config = new Dictionary<string, string>(),
        CreatedAt = DateTime.UtcNow
    };

    [Fact]
    public void MirrorProperties_DefaultFalse()
    {
        // Act
        var metadata = CreateMetadata();

        // Assert
        Assert.False(metadata.IsMirror);
        Assert.False(metadata.IsReadOnly);
        Assert.Null(metadata.SourceLinkId);
    }

    [Fact]
    public void MirrorProperties_SetAndGet()
    {
        // Arrange
        var metadata = CreateMetadata();

        // Act
        metadata.IsMirror = true;
        metadata.IsReadOnly = true;
        metadata.SourceLinkId = "link-1";

        // Assert
        Assert.True(metadata.IsMirror);
        Assert.True(metadata.IsReadOnly);
        Assert.Equal("link-1", metadata.SourceLinkId);
    }

    [Fact]
    public void MirrorProperties_IndependentFlags()
    {
        // Arrange
        var metadata = CreateMetadata();

        // Act - set IsMirror but not IsReadOnly
        metadata.IsMirror = true;

        // Assert
        Assert.True(metadata.IsMirror);
        Assert.False(metadata.IsReadOnly);

        // Act - set IsReadOnly but clear IsMirror
        metadata.IsMirror = false;
        metadata.IsReadOnly = true;

        // Assert
        Assert.False(metadata.IsMirror);
        Assert.True(metadata.IsReadOnly);
    }
}
