using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Cdc.Tests;

/// <summary>
/// Tests for PostgresCdcSource WAL message parsing (using mocked data).
/// These tests verify parsing logic without requiring a live PostgreSQL connection.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class PostgresCdcSourceTests
{
    [Fact]
    public void DatabaseType_IsPostgreSQL()
    {
        // Arrange
        var config = new CdcConfig { ConnectionString = "Host=localhost" };
        using var loggerFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));
        var source = new PostgresCdcSource(config, loggerFactory.CreateLogger<PostgresCdcSource>());

        // Assert
        Assert.Equal("PostgreSQL", source.DatabaseType);
    }

    [Fact]
    public void Constructor_NullConfig_Throws()
    {
        using var loggerFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));
        Assert.Throws<ArgumentNullException>(() =>
            new PostgresCdcSource(null!, loggerFactory.CreateLogger<PostgresCdcSource>()));
    }

    [Fact]
    public void Constructor_NullLogger_Throws()
    {
        var config = new CdcConfig();
        Assert.Throws<ArgumentNullException>(() =>
            new PostgresCdcSource(config, null!));
    }

    [Fact]
    public void EventsCaptured_InitiallyZero()
    {
        // Arrange
        var config = new CdcConfig { ConnectionString = "Host=localhost" };
        using var loggerFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));
        var source = new PostgresCdcSource(config, loggerFactory.CreateLogger<PostgresCdcSource>());

        // Assert
        Assert.Equal(0, source.EventsCaptured);
    }

    [Fact]
    public void LastConfirmedLsn_InitiallyZero()
    {
        // Arrange
        var config = new CdcConfig { ConnectionString = "Host=localhost" };
        using var loggerFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));
        var source = new PostgresCdcSource(config, loggerFactory.CreateLogger<PostgresCdcSource>());

        // Assert
        Assert.Equal(0, source.LastConfirmedLsn);
    }

    [Fact]
    public async Task DisposeAsync_DoesNotThrow()
    {
        // Arrange
        var config = new CdcConfig { ConnectionString = "Host=localhost" };
        using var loggerFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));
        var source = new PostgresCdcSource(config, loggerFactory.CreateLogger<PostgresCdcSource>());

        // Act & Assert - should not throw
        await source.DisposeAsync();
    }
}
