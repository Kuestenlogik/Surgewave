using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Schema.Registry.Client;
using Kuestenlogik.Surgewave.Schema.Registry.Serdes.Avro;
using Kuestenlogik.Surgewave.Schema.Registry.Serdes.Json;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests.Serialization;

/// <summary>
/// Tests for schema registry builder extensions.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class SchemaRegistryBuilderExtensionsTests
{
    private readonly MockSchemaRegistry _schemaRegistry = new();

    public record TestRecord(string Name, int Value);

    #region Avro Extension Tests

    [Fact]
    public void WithAvroValueSerializer_SetsAsyncValueSerializer()
    {
        // Arrange
        var options = new ProducerOptions<string, TestRecord>();

        // Act
        options.WithAvroValueSerializer(_schemaRegistry);

        // Assert
        Assert.NotNull(options.AsyncValueSerializer);
        Assert.IsType<SchemaRegistryAvroSerializer<TestRecord>>(options.AsyncValueSerializer);
    }

    [Fact]
    public void WithAvroKeySerializer_SetsAsyncKeySerializer()
    {
        // Arrange
        var options = new ProducerOptions<TestRecord, string>();

        // Act
        options.WithAvroKeySerializer(_schemaRegistry);

        // Assert
        Assert.NotNull(options.AsyncKeySerializer);
        Assert.IsType<SchemaRegistryAvroSerializer<TestRecord>>(options.AsyncKeySerializer);
    }

    [Fact]
    public void WithAvroValueDeserializer_SetsAsyncValueDeserializer()
    {
        // Arrange
        var options = new ConsumerOptions<string, TestRecord>();

        // Act
        options.WithAvroValueDeserializer(_schemaRegistry);

        // Assert
        Assert.NotNull(options.AsyncValueDeserializer);
        Assert.IsType<SchemaRegistryAvroDeserializer<TestRecord>>(options.AsyncValueDeserializer);
    }

    [Fact]
    public void WithAvroKeyDeserializer_SetsAsyncKeyDeserializer()
    {
        // Arrange
        var options = new ConsumerOptions<TestRecord, string>();

        // Act
        options.WithAvroKeyDeserializer(_schemaRegistry);

        // Assert
        Assert.NotNull(options.AsyncKeyDeserializer);
        Assert.IsType<SchemaRegistryAvroDeserializer<TestRecord>>(options.AsyncKeyDeserializer);
    }

    [Fact]
    public void WithAvroSerializers_SetsBothKeyAndValueSerializers()
    {
        // Arrange
        var options = new ProducerOptions<TestRecord, TestRecord>();

        // Act
        options.WithAvroSerializers(_schemaRegistry);

        // Assert
        Assert.NotNull(options.AsyncKeySerializer);
        Assert.NotNull(options.AsyncValueSerializer);
    }

    [Fact]
    public void WithAvroDeserializers_SetsBothKeyAndValueDeserializers()
    {
        // Arrange
        var options = new ConsumerOptions<TestRecord, TestRecord>();

        // Act
        options.WithAvroDeserializers(_schemaRegistry);

        // Assert
        Assert.NotNull(options.AsyncKeyDeserializer);
        Assert.NotNull(options.AsyncValueDeserializer);
    }

    [Fact]
    public void WithAvroValueSerializer_ConfigureCallback_IsInvoked()
    {
        // Arrange
        var options = new ProducerOptions<string, TestRecord>();
        var configureInvoked = false;

        // Act
        options.WithAvroValueSerializer(_schemaRegistry, config =>
        {
            configureInvoked = true;
            config.AutoRegisterSchemas = false;
        });

        // Assert
        Assert.True(configureInvoked);
    }

    [Fact]
    public void WithAvroValueSerializer_ReturnsSameOptionsForChaining()
    {
        // Arrange
        var options = new ProducerOptions<string, TestRecord>();

        // Act
        var result = options.WithAvroValueSerializer(_schemaRegistry);

        // Assert
        Assert.Same(options, result);
    }

    #endregion

    #region JSON Schema Extension Tests

    [Fact]
    public void WithJsonSchemaValueSerializer_SetsAsyncValueSerializer()
    {
        // Arrange
        var options = new ProducerOptions<string, TestRecord>();

        // Act
        options.WithJsonSchemaValueSerializer(_schemaRegistry);

        // Assert
        Assert.NotNull(options.AsyncValueSerializer);
        Assert.IsType<SchemaRegistryJsonSerializer<TestRecord>>(options.AsyncValueSerializer);
    }

    [Fact]
    public void WithJsonSchemaKeySerializer_SetsAsyncKeySerializer()
    {
        // Arrange
        var options = new ProducerOptions<TestRecord, string>();

        // Act
        options.WithJsonSchemaKeySerializer(_schemaRegistry);

        // Assert
        Assert.NotNull(options.AsyncKeySerializer);
        Assert.IsType<SchemaRegistryJsonSerializer<TestRecord>>(options.AsyncKeySerializer);
    }

    [Fact]
    public void WithJsonSchemaValueDeserializer_SetsAsyncValueDeserializer()
    {
        // Arrange
        var options = new ConsumerOptions<string, TestRecord>();

        // Act
        options.WithJsonSchemaValueDeserializer(_schemaRegistry);

        // Assert
        Assert.NotNull(options.AsyncValueDeserializer);
        Assert.IsType<SchemaRegistryJsonDeserializer<TestRecord>>(options.AsyncValueDeserializer);
    }

    [Fact]
    public void WithJsonSchemaSerializers_SetsBothKeyAndValueSerializers()
    {
        // Arrange
        var options = new ProducerOptions<TestRecord, TestRecord>();

        // Act
        options.WithJsonSchemaSerializers(_schemaRegistry);

        // Assert
        Assert.NotNull(options.AsyncKeySerializer);
        Assert.NotNull(options.AsyncValueSerializer);
    }

    [Fact]
    public void WithJsonSchemaDeserializers_SetsBothKeyAndValueDeserializers()
    {
        // Arrange
        var options = new ConsumerOptions<TestRecord, TestRecord>();

        // Act
        options.WithJsonSchemaDeserializers(_schemaRegistry);

        // Assert
        Assert.NotNull(options.AsyncKeyDeserializer);
        Assert.NotNull(options.AsyncValueDeserializer);
    }

    [Fact]
    public void WithJsonSchemaValueSerializer_ConfigureCallback_IsInvoked()
    {
        // Arrange
        var options = new ProducerOptions<string, TestRecord>();
        var configuredValidation = false;

        // Act
        options.WithJsonSchemaValueSerializer(_schemaRegistry, config =>
        {
            config.ValidateOnSerialize = true;
            configuredValidation = true;
        });

        // Assert
        Assert.True(configuredValidation);
    }

    #endregion

    #region Chaining Tests

    [Fact]
    public void Extensions_CanBeChainedWithOtherOptions()
    {
        // Arrange & Act
        var options = new ProducerOptions<string, TestRecord>()
            .WithAvroValueSerializer(_schemaRegistry, config => config.AutoRegisterSchemas = false);

        // Set other options
        options.BatchSize = 500;
        options.LingerMs = 10;

        // Assert
        Assert.NotNull(options.AsyncValueSerializer);
        Assert.Equal(500, options.BatchSize);
        Assert.Equal(10, options.LingerMs);
    }

    [Fact]
    public void ConsumerExtensions_CanBeChainedWithOtherOptions()
    {
        // Arrange & Act
        var options = new ConsumerOptions<string, TestRecord>()
            .WithAvroValueDeserializer(_schemaRegistry);

        // Set other options
        options.GroupId = "test-group";
        options.EnableAutoCommit = false;

        // Assert
        Assert.NotNull(options.AsyncValueDeserializer);
        Assert.Equal("test-group", options.GroupId);
        Assert.False(options.EnableAutoCommit);
    }

    #endregion

    #region Subject Name Strategy Tests

    [Fact]
    public void WithAvroValueSerializer_CanConfigureSubjectNameStrategy()
    {
        // Arrange
        var options = new ProducerOptions<string, TestRecord>();

        // Act
        options.WithAvroValueSerializer(_schemaRegistry, config =>
        {
            config.SubjectNameStrategy = RecordNameStrategy.Instance;
        });

        // Assert
        Assert.NotNull(options.AsyncValueSerializer);
    }

    [Fact]
    public void WithJsonSchemaValueSerializer_CanConfigureSubjectNameStrategy()
    {
        // Arrange
        var options = new ProducerOptions<string, TestRecord>();

        // Act
        options.WithJsonSchemaValueSerializer(_schemaRegistry, config =>
        {
            config.SubjectNameStrategy = TopicRecordNameStrategy.Instance;
        });

        // Assert
        Assert.NotNull(options.AsyncValueSerializer);
    }

    #endregion
}
