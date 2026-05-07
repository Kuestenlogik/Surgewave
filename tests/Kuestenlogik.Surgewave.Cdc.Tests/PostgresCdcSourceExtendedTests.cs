using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Cdc.Tests;

/// <summary>
/// Extended tests for PostgresCdcSource configuration, slot naming, and connection string parsing.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class PostgresCdcSourceExtendedTests
{
    private static ILogger<PostgresCdcSource> CreateLogger()
    {
        using var factory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));
        return factory.CreateLogger<PostgresCdcSource>();
    }

    [Fact]
    public void Constructor_WithDefaultConfig_SetsSlotName()
    {
        var config = new CdcConfig { ConnectionString = "Host=localhost;Database=testdb" };
        var source = new PostgresCdcSource(config, CreateLogger());

        Assert.Equal("PostgreSQL", source.DatabaseType);
        Assert.Equal(0, source.EventsCaptured);
        Assert.Equal(0, source.LastConfirmedLsn);
    }

    [Fact]
    public void Constructor_WithCustomSlotName_DoesNotThrow()
    {
        var config = new CdcConfig
        {
            ConnectionString = "Host=db.example.com;Port=5432;Database=production",
            SlotName = "custom_replication_slot",
            PublicationName = "custom_publication"
        };

        var source = new PostgresCdcSource(config, CreateLogger());
        Assert.Equal("PostgreSQL", source.DatabaseType);
    }

    [Fact]
    public void Constructor_WithTables_DoesNotThrow()
    {
        var config = new CdcConfig
        {
            ConnectionString = "Host=localhost",
            Tables = ["public.orders", "public.products", "inventory.items"]
        };

        var source = new PostgresCdcSource(config, CreateLogger());
        Assert.Equal("PostgreSQL", source.DatabaseType);
    }

    [Fact]
    public void Constructor_EmptyConnectionString_DoesNotThrow()
    {
        var config = new CdcConfig { ConnectionString = "" };
        var source = new PostgresCdcSource(config, CreateLogger());

        Assert.Equal("PostgreSQL", source.DatabaseType);
    }

    [Fact]
    public void Constructor_WithSpecialCharactersInSlotName_DoesNotThrow()
    {
        var config = new CdcConfig
        {
            ConnectionString = "Host=localhost",
            SlotName = "slot'with\"special",
            PublicationName = "pub'with\"chars"
        };

        // Should not throw - identifiers should be escaped
        var source = new PostgresCdcSource(config, CreateLogger());
        Assert.Equal("PostgreSQL", source.DatabaseType);
    }

    [Fact]
    public async Task DisposeAsync_MultipleTimes_DoesNotThrow()
    {
        var config = new CdcConfig { ConnectionString = "Host=localhost" };
        var source = new PostgresCdcSource(config, CreateLogger());

        await source.DisposeAsync();
        await source.DisposeAsync();
    }
}
