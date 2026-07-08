using Kuestenlogik.Surgewave.Schema.Registry.Client;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests.Serialization;

/// <summary>
/// Tests for subject name strategies.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class SubjectNameStrategyTests
{
    #region TopicNameStrategy Tests

    [Theory]
    [InlineData("my-topic", false, "my-topic-value")]
    [InlineData("my-topic", true, "my-topic-key")]
    [InlineData("orders", false, "orders-value")]
    [InlineData("orders", true, "orders-key")]
    [InlineData("user.events", false, "user.events-value")]
    [InlineData("user.events", true, "user.events-key")]
    public void TopicNameStrategy_GetSubjectName_ReturnsCorrectFormat(
        string topic,
        bool isKey,
        string expected)
    {
        // Arrange
        var strategy = TopicNameStrategy.Instance;

        // Act
        var result = strategy.GetSubjectName(topic, isKey);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void TopicNameStrategy_IgnoresRecordName()
    {
        // Arrange
        var strategy = TopicNameStrategy.Instance;

        // Act - should ignore the record name
        var result = strategy.GetSubjectName("my-topic", false, "com.example.MyRecord");

        // Assert - should only use topic name
        Assert.Equal("my-topic-value", result);
    }

    [Fact]
    public void TopicNameStrategy_IsSingleton()
    {
        // Assert
        Assert.Same(TopicNameStrategy.Instance, TopicNameStrategy.Instance);
    }

    #endregion

    #region RecordNameStrategy Tests

    [Theory]
    [InlineData("com.example.User", "com.example.User")]
    [InlineData("OrderEvent", "OrderEvent")]
    [InlineData("io.surgewave.events.ClickEvent", "io.surgewave.events.ClickEvent")]
    public void RecordNameStrategy_GetSubjectName_ReturnsRecordName(
        string recordName,
        string expected)
    {
        // Arrange
        var strategy = RecordNameStrategy.Instance;

        // Act
        var result = strategy.GetSubjectName("any-topic", false, recordName);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void RecordNameStrategy_NullRecordName_ThrowsArgumentException()
    {
        // Arrange
        var strategy = RecordNameStrategy.Instance;

        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
            strategy.GetSubjectName("my-topic", false, null));
    }

    [Fact]
    public void RecordNameStrategy_EmptyRecordName_ThrowsArgumentException()
    {
        // Arrange
        var strategy = RecordNameStrategy.Instance;

        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
            strategy.GetSubjectName("my-topic", false, ""));
    }

    [Fact]
    public void RecordNameStrategy_IgnoresTopicAndIsKey()
    {
        // Arrange
        var strategy = RecordNameStrategy.Instance;

        // Act - topic name and isKey should be ignored
        var result1 = strategy.GetSubjectName("topic1", false, "MyRecord");
        var result2 = strategy.GetSubjectName("topic2", true, "MyRecord");

        // Assert - both should return the same record name
        Assert.Equal("MyRecord", result1);
        Assert.Equal("MyRecord", result2);
    }

    [Fact]
    public void RecordNameStrategy_IsSingleton()
    {
        // Assert
        Assert.Same(RecordNameStrategy.Instance, RecordNameStrategy.Instance);
    }

    #endregion

    #region TopicRecordNameStrategy Tests

    [Theory]
    [InlineData("my-topic", "User", "my-topic-User")]
    [InlineData("orders", "OrderEvent", "orders-OrderEvent")]
    [InlineData("events", "com.example.ClickEvent", "events-com.example.ClickEvent")]
    public void TopicRecordNameStrategy_GetSubjectName_ReturnsCombinedName(
        string topic,
        string recordName,
        string expected)
    {
        // Arrange
        var strategy = TopicRecordNameStrategy.Instance;

        // Act
        var result = strategy.GetSubjectName(topic, false, recordName);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void TopicRecordNameStrategy_NullRecordName_ThrowsArgumentException()
    {
        // Arrange
        var strategy = TopicRecordNameStrategy.Instance;

        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
            strategy.GetSubjectName("my-topic", false, null));
    }

    [Fact]
    public void TopicRecordNameStrategy_IgnoresIsKey()
    {
        // Arrange
        var strategy = TopicRecordNameStrategy.Instance;

        // Act - isKey should be ignored
        var result1 = strategy.GetSubjectName("topic", false, "Record");
        var result2 = strategy.GetSubjectName("topic", true, "Record");

        // Assert - both should return the same combined name
        Assert.Equal("topic-Record", result1);
        Assert.Equal("topic-Record", result2);
    }

    [Fact]
    public void TopicRecordNameStrategy_IsSingleton()
    {
        // Assert
        Assert.Same(TopicRecordNameStrategy.Instance, TopicRecordNameStrategy.Instance);
    }

    #endregion

    #region SubjectNameStrategies Factory Tests

    [Theory]
    [InlineData(SubjectNameStrategyType.TopicName)]
    [InlineData(SubjectNameStrategyType.RecordName)]
    [InlineData(SubjectNameStrategyType.TopicRecordName)]
    public void SubjectNameStrategies_Get_ReturnsCorrectStrategy(SubjectNameStrategyType type)
    {
        // Act
        var strategy = SubjectNameStrategies.Get(type);

        // Assert
        Assert.NotNull(strategy);

        var expectedType = type switch
        {
            SubjectNameStrategyType.TopicName => typeof(TopicNameStrategy),
            SubjectNameStrategyType.RecordName => typeof(RecordNameStrategy),
            SubjectNameStrategyType.TopicRecordName => typeof(TopicRecordNameStrategy),
            _ => throw new ArgumentOutOfRangeException(nameof(type))
        };

        Assert.IsType(expectedType, strategy);
    }

    [Fact]
    public void SubjectNameStrategies_Get_ReturnsSingletonInstances()
    {
        // Act
        var strategy1 = SubjectNameStrategies.Get(SubjectNameStrategyType.TopicName);
        var strategy2 = SubjectNameStrategies.Get(SubjectNameStrategyType.TopicName);

        // Assert
        Assert.Same(strategy1, strategy2);
    }

    [Fact]
    public void SubjectNameStrategies_InvalidType_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        var invalidType = (SubjectNameStrategyType)999;

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            SubjectNameStrategies.Get(invalidType));
    }

    #endregion
}
