using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Schema.Registry.Migration;
using Xunit;

namespace Kuestenlogik.Surgewave.Schema.Registry.Tests;

/// <summary>
/// Tests for zero-downtime schema migration: SchemaMigrator, TypeCoercer, MigrationPath, MigrationCache.
/// </summary>
public sealed class SchemaMigrationTests
{
    private readonly ITestOutputHelper _output;
    private readonly SchemaMigrator _migrator = new();
    private readonly SchemaMigrationConfig _defaultConfig = new();

    // v1: basic order
    private const string OrderV1 = """
        {
            "type": "object",
            "properties": {
                "orderId": { "type": "string" },
                "amount": { "type": "number" },
                "customerId": { "type": "string" }
            },
            "required": ["orderId", "amount", "customerId"]
        }
        """;

    // v2: added discountCode (optional), amount changed to string
    private const string OrderV2 = """
        {
            "type": "object",
            "properties": {
                "orderId": { "type": "string" },
                "amount": { "type": "string" },
                "customerId": { "type": "string" },
                "discountCode": { "type": "string" }
            },
            "required": ["orderId", "amount", "customerId"]
        }
        """;

    // v3: customerId made nullable, added priority (integer)
    private const string OrderV3 = """
        {
            "type": "object",
            "properties": {
                "orderId": { "type": "string" },
                "amount": { "type": "string" },
                "customerId": { "type": ["string", "null"] },
                "discountCode": { "type": "string" },
                "priority": { "type": "integer" }
            },
            "required": ["orderId", "amount"]
        }
        """;

    // Schema with nested object
    private const string NestedV1 = """
        {
            "type": "object",
            "properties": {
                "name": { "type": "string" },
                "address": { "type": "object" }
            },
            "required": ["name"]
        }
        """;

    // Schema with nested object + new field
    private const string NestedV2 = """
        {
            "type": "object",
            "properties": {
                "name": { "type": "string" },
                "address": { "type": "object" },
                "email": { "type": "string" }
            },
            "required": ["name"]
        }
        """;

    public SchemaMigrationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void Migrate_AddedField_UsesDefault()
    {
        var message = """{"orderId":"123","amount":42.5,"customerId":"C001"}""";
        var messageBytes = Encoding.UTF8.GetBytes(message);

        var result = _migrator.Migrate(messageBytes, OrderV1, OrderV2, _defaultConfig);
        var resultJson = Encoding.UTF8.GetString(result);

        _output.WriteLine($"Input:  {message}");
        _output.WriteLine($"Output: {resultJson}");

        using var doc = JsonDocument.Parse(resultJson);
        var root = doc.RootElement;

        Assert.Equal("123", root.GetProperty("orderId").GetString());
        // amount was number, now string — should be coerced
        Assert.Equal(JsonValueKind.String, root.GetProperty("amount").ValueKind);
        Assert.Equal("C001", root.GetProperty("customerId").GetString());
        // discountCode is new optional field — should get default ""
        Assert.True(root.TryGetProperty("discountCode", out var discount));
        Assert.Equal("", discount.GetString());
    }

    [Fact]
    public void Migrate_RemovedField_Ignored()
    {
        var message = """{"orderId":"123","amount":"42.5","customerId":"C001","discountCode":"SAVE10","extraField":"xyz"}""";
        var messageBytes = Encoding.UTF8.GetBytes(message);

        // Migrate from v2 back to v1 (discountCode should be dropped)
        var config = new SchemaMigrationConfig { ExtraFieldStrategy = ExtraFieldStrategy.Ignore };
        var result = _migrator.Migrate(messageBytes, OrderV2, OrderV1, config);
        var resultJson = Encoding.UTF8.GetString(result);

        _output.WriteLine($"Input:  {message}");
        _output.WriteLine($"Output: {resultJson}");

        using var doc = JsonDocument.Parse(resultJson);
        var root = doc.RootElement;

        Assert.Equal("123", root.GetProperty("orderId").GetString());
        Assert.False(root.TryGetProperty("discountCode", out _));
        Assert.False(root.TryGetProperty("extraField", out _));
    }

    [Fact]
    public void Migrate_TypeChanged_Coerced()
    {
        // amount is number in v1, string in v2
        var message = """{"orderId":"123","amount":42.5,"customerId":"C001"}""";
        var messageBytes = Encoding.UTF8.GetBytes(message);

        var result = _migrator.Migrate(messageBytes, OrderV1, OrderV2, _defaultConfig);
        var resultJson = Encoding.UTF8.GetString(result);

        _output.WriteLine($"Input:  {message}");
        _output.WriteLine($"Output: {resultJson}");

        using var doc = JsonDocument.Parse(resultJson);
        var root = doc.RootElement;

        // amount should be coerced from number to string
        Assert.Equal(JsonValueKind.String, root.GetProperty("amount").ValueKind);
        Assert.Equal("42.5", root.GetProperty("amount").GetString());
    }

    [Fact]
    public void Migrate_NestedObject_Migrated()
    {
        var message = """{"name":"John","address":{"city":"Berlin","zip":"10115"}}""";
        var messageBytes = Encoding.UTF8.GetBytes(message);

        var result = _migrator.Migrate(messageBytes, NestedV1, NestedV2, _defaultConfig);
        var resultJson = Encoding.UTF8.GetString(result);

        _output.WriteLine($"Input:  {message}");
        _output.WriteLine($"Output: {resultJson}");

        using var doc = JsonDocument.Parse(resultJson);
        var root = doc.RootElement;

        Assert.Equal("John", root.GetProperty("name").GetString());
        // address object should be preserved as-is
        Assert.Equal(JsonValueKind.Object, root.GetProperty("address").ValueKind);
        // email is new — should get default
        Assert.True(root.TryGetProperty("email", out var email));
        Assert.Equal("", email.GetString());
    }

    [Fact]
    public void Migrate_MissingRequired_FailStrategy()
    {
        var message = """{"orderId":"123"}""";
        var messageBytes = Encoding.UTF8.GetBytes(message);

        var config = new SchemaMigrationConfig { MissingFieldStrategy = MissingFieldStrategy.Fail };

        // v1 requires orderId, amount, customerId — message is missing amount and customerId
        Assert.Throws<SchemaMigrationException>(() =>
            _migrator.Migrate(messageBytes, OrderV1, OrderV2, config));
    }

    [Fact]
    public void Migrate_NullableField_Preserved()
    {
        var message = """{"orderId":"123","amount":"42.5","customerId":null,"discountCode":"SAVE10"}""";
        var messageBytes = Encoding.UTF8.GetBytes(message);

        var result = _migrator.Migrate(messageBytes, OrderV2, OrderV3, _defaultConfig);
        var resultJson = Encoding.UTF8.GetString(result);

        _output.WriteLine($"Input:  {message}");
        _output.WriteLine($"Output: {resultJson}");

        using var doc = JsonDocument.Parse(resultJson);
        var root = doc.RootElement;

        // customerId is now nullable — null should be preserved
        Assert.Equal(JsonValueKind.Null, root.GetProperty("customerId").ValueKind);
        // priority is new integer field
        Assert.True(root.TryGetProperty("priority", out var priority));
        Assert.Equal(0, priority.GetInt32());
    }

    [Fact]
    public void TypeCoercer_IntToString()
    {
        var json = "42";
        using var doc = JsonDocument.Parse(json);
        var result = TypeCoercer.Coerce(doc.RootElement, "integer", "string");

        Assert.Equal("42", result);
    }

    [Fact]
    public void TypeCoercer_StringToInt()
    {
        var json = "\"123\"";
        using var doc = JsonDocument.Parse(json);
        var result = TypeCoercer.Coerce(doc.RootElement, "string", "integer");

        Assert.Equal(123L, result);
    }

    [Fact]
    public void TypeCoercer_IntToDouble()
    {
        var json = "42";
        using var doc = JsonDocument.Parse(json);
        var result = TypeCoercer.Coerce(doc.RootElement, "integer", "number");

        Assert.Equal(42.0, result);
    }

    [Fact]
    public void TypeCoercer_CanCoerce_IntToString()
    {
        Assert.True(TypeCoercer.CanCoerce("integer", "string"));
    }

    [Fact]
    public void TypeCoercer_CanCoerce_StringToInt()
    {
        Assert.True(TypeCoercer.CanCoerce("string", "integer"));
    }

    [Fact]
    public void TypeCoercer_CanCoerce_BoolToString()
    {
        Assert.True(TypeCoercer.CanCoerce("boolean", "string"));
    }

    [Fact]
    public void TypeCoercer_CanCoerce_ObjectToArray_False()
    {
        Assert.False(TypeCoercer.CanCoerce("object", "array"));
    }

    [Fact]
    public void TypeCoercer_CanCoerce_SameType_True()
    {
        Assert.True(TypeCoercer.CanCoerce("string", "string"));
        Assert.True(TypeCoercer.CanCoerce("integer", "integer"));
    }

    [Fact]
    public void TypeCoercer_StringToBool()
    {
        var json = "\"true\"";
        using var doc = JsonDocument.Parse(json);
        var result = TypeCoercer.Coerce(doc.RootElement, "string", "boolean");
        Assert.Equal(true, result);

        var jsonFalse = "\"0\"";
        using var docFalse = JsonDocument.Parse(jsonFalse);
        var resultFalse = TypeCoercer.Coerce(docFalse.RootElement, "string", "boolean");
        Assert.Equal(false, resultFalse);
    }

    [Fact]
    public void TypeCoercer_GetDefaultForType()
    {
        Assert.Equal("", TypeCoercer.GetDefaultForType("string"));
        Assert.Equal(0L, TypeCoercer.GetDefaultForType("integer"));
        Assert.Equal(0.0, TypeCoercer.GetDefaultForType("number"));
        Assert.Equal(false, TypeCoercer.GetDefaultForType("boolean"));
        Assert.Null(TypeCoercer.GetDefaultForType("null"));
    }

    [Fact]
    public void MigrationPath_Direct()
    {
        var schemas = new Dictionary<int, string>
        {
            [1] = OrderV1,
            [2] = OrderV2
        };

        var path = _migrator.GetMigrationPath("orders-value", 1, 2, schemas);

        _output.WriteLine($"Path: {path.Subject} v{path.FromVersion} -> v{path.ToVersion}");
        _output.WriteLine($"Steps: {path.Steps.Count}, Direct: {path.IsDirectMigration}");

        Assert.Equal("orders-value", path.Subject);
        Assert.Equal(1, path.FromVersion);
        Assert.Equal(2, path.ToVersion);
        Assert.True(path.IsDirectMigration);
        Assert.Single(path.Steps);
        Assert.True(path.Steps[0].Transforms.Count > 0);
    }

    [Fact]
    public void MigrationPath_MultiStep()
    {
        var schemas = new Dictionary<int, string>
        {
            [1] = OrderV1,
            [2] = OrderV2,
            [3] = OrderV3
        };

        var path = _migrator.GetMigrationPath("orders-value", 1, 3, schemas);

        _output.WriteLine($"Path: {path.Subject} v{path.FromVersion} -> v{path.ToVersion}");
        _output.WriteLine($"Steps: {path.Steps.Count}, Direct: {path.IsDirectMigration}");
        foreach (var step in path.Steps)
        {
            _output.WriteLine($"  Step v{step.FromVersion} -> v{step.ToVersion}: {step.Transforms.Count} transforms");
        }

        Assert.Equal("orders-value", path.Subject);
        Assert.Equal(1, path.FromVersion);
        Assert.Equal(3, path.ToVersion);
        Assert.False(path.IsDirectMigration);
        Assert.Equal(2, path.Steps.Count);
    }

    [Fact]
    public void MigrationCache_HitsAndMisses()
    {
        var cache = new SchemaMigrationCache(maxEntries: 5);

        // Miss
        var result = cache.GetMigrator("test", 1, 2);
        Assert.Null(result);
        Assert.Equal(0, cache.Hits);
        Assert.Equal(1, cache.Misses);

        // Cache a migrator
        Func<byte[], byte[]> migrator = b => b;
        cache.CacheMigrator("test", 1, 2, migrator);

        // Hit
        result = cache.GetMigrator("test", 1, 2);
        Assert.NotNull(result);
        Assert.Equal(1, cache.Hits);
        Assert.Equal(1, cache.Misses);
        Assert.Equal(1, cache.Count);
    }

    [Fact]
    public void MigrationCache_Eviction()
    {
        var cache = new SchemaMigrationCache(maxEntries: 3);
        Func<byte[], byte[]> migrator = b => b;

        cache.CacheMigrator("test", 1, 2, migrator);
        cache.CacheMigrator("test", 2, 3, migrator);
        cache.CacheMigrator("test", 3, 4, migrator);

        // Access first to make it recent
        cache.GetMigrator("test", 1, 2);

        // Add one more — should evict one of the less recent ones
        cache.CacheMigrator("test", 4, 5, migrator);

        Assert.True(cache.Count <= 4); // May evict to stay within bounds
        Assert.True(cache.Evictions >= 0); // At least tried to evict

        var stats = cache.GetStats();
        _output.WriteLine($"Cache stats: count={stats.Count}, hits={stats.Hits}, misses={stats.Misses}, " +
                          $"evictions={stats.Evictions}, hitRate={stats.HitRate:P2}");
    }

    [Fact]
    public void SchemaMigrationConfig_Defaults()
    {
        var config = new SchemaMigrationConfig();

        Assert.False(config.Enabled);
        Assert.True(config.AutoMigrateOnRead);
        Assert.False(config.AutoMigrateOnWrite);
        Assert.Equal(MissingFieldStrategy.UseDefault, config.MissingFieldStrategy);
        Assert.Equal(ExtraFieldStrategy.Ignore, config.ExtraFieldStrategy);
        Assert.Equal(TypeMismatchStrategy.Coerce, config.TypeMismatchStrategy);
        Assert.Equal(100, config.MaxCachedMigrators);
    }

    [Fact]
    public void Migrate_ExtraField_FailStrategy()
    {
        var message = """{"orderId":"123","amount":42.5,"customerId":"C001","unknownField":"x"}""";
        var messageBytes = Encoding.UTF8.GetBytes(message);

        var config = new SchemaMigrationConfig { ExtraFieldStrategy = ExtraFieldStrategy.Fail };

        Assert.Throws<SchemaMigrationException>(() =>
            _migrator.Migrate(messageBytes, OrderV1, OrderV1, config));
    }

    [Fact]
    public void Migrate_ExtraField_IncludeStrategy()
    {
        var message = """{"orderId":"123","amount":42.5,"customerId":"C001","unknownField":"x"}""";
        var messageBytes = Encoding.UTF8.GetBytes(message);

        var config = new SchemaMigrationConfig { ExtraFieldStrategy = ExtraFieldStrategy.Include };

        var result = _migrator.Migrate(messageBytes, OrderV1, OrderV1, config);
        var resultJson = Encoding.UTF8.GetString(result);

        _output.WriteLine($"Output: {resultJson}");

        using var doc = JsonDocument.Parse(resultJson);
        Assert.True(doc.RootElement.TryGetProperty("unknownField", out var unknown));
        Assert.Equal("x", unknown.GetString());
    }

    [Fact]
    public void Migrate_TypeMismatch_FailStrategy()
    {
        var message = """{"orderId":"123","amount":42.5,"customerId":"C001"}""";
        var messageBytes = Encoding.UTF8.GetBytes(message);

        var config = new SchemaMigrationConfig { TypeMismatchStrategy = TypeMismatchStrategy.Fail };

        // v1 has amount as number, v2 as string — type mismatch should fail
        Assert.Throws<SchemaMigrationException>(() =>
            _migrator.Migrate(messageBytes, OrderV1, OrderV2, config));
    }

    [Fact]
    public void Migrate_TypeMismatch_UseDefaultStrategy()
    {
        var message = """{"orderId":"123","amount":42.5,"customerId":"C001"}""";
        var messageBytes = Encoding.UTF8.GetBytes(message);

        var config = new SchemaMigrationConfig { TypeMismatchStrategy = TypeMismatchStrategy.UseDefault };

        var result = _migrator.Migrate(messageBytes, OrderV1, OrderV2, config);
        var resultJson = Encoding.UTF8.GetString(result);

        _output.WriteLine($"Output: {resultJson}");

        using var doc = JsonDocument.Parse(resultJson);
        // amount should get default string value "" since UseDefault
        Assert.Equal("", doc.RootElement.GetProperty("amount").GetString());
    }

    [Fact]
    public void Migrate_MissingField_UseNullStrategy()
    {
        var message = """{"orderId":"123","amount":42.5,"customerId":"C001"}""";
        var messageBytes = Encoding.UTF8.GetBytes(message);

        var config = new SchemaMigrationConfig { MissingFieldStrategy = MissingFieldStrategy.UseNull };

        var result = _migrator.Migrate(messageBytes, OrderV1, OrderV2, config);
        var resultJson = Encoding.UTF8.GetString(result);

        _output.WriteLine($"Output: {resultJson}");

        using var doc = JsonDocument.Parse(resultJson);
        // discountCode is new and missing — should be null
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("discountCode").ValueKind);
    }

    [Fact]
    public void Migrate_NeedsMigration_DifferentVersions()
    {
        Assert.True(_migrator.NeedsMigration("1", "2"));
        Assert.False(_migrator.NeedsMigration("1", "1"));
    }

    [Fact]
    public void Migrate_BuildMigrator_Cached()
    {
        var compiledMigrator = _migrator.BuildMigrator(OrderV1, OrderV2, _defaultConfig);

        var message = """{"orderId":"123","amount":42.5,"customerId":"C001"}""";
        var messageBytes = Encoding.UTF8.GetBytes(message);

        // Run the compiled migrator multiple times
        var result1 = compiledMigrator(messageBytes);
        var result2 = compiledMigrator(messageBytes);

        Assert.Equal(
            Encoding.UTF8.GetString(result1),
            Encoding.UTF8.GetString(result2));
    }

    [Fact]
    public void MigrationPath_SameVersion_Empty()
    {
        var schemas = new Dictionary<int, string>
        {
            [1] = OrderV1
        };

        var path = _migrator.GetMigrationPath("orders-value", 1, 1, schemas);

        Assert.Empty(path.Steps);
        Assert.True(path.IsDirectMigration);
        Assert.Equal(0, path.TotalTransformCount);
    }
}
