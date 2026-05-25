using Kuestenlogik.Surgewave.Client.Validation;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests;

/// <summary>
/// Tests for validation utilities: TopicNameValidator, BootstrapServerValidator, Guard, ValidationResult.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ValidationTests
{
    #region ValidationResult Tests

    [Fact]
    public void ValidationResult_Success_IsValid()
    {
        var result = ValidationResult.Success;
        Assert.True(result.IsValid);
        Assert.Null(result.ErrorMessage);
    }

    [Fact]
    public void ValidationResult_Error_IsNotValid()
    {
        var result = ValidationResult.Error("some error");
        Assert.False(result.IsValid);
        Assert.Equal("some error", result.ErrorMessage);
    }

    [Fact]
    public void ValidationResult_ImplicitBoolConversion()
    {
        ValidationResult success = ValidationResult.Success;
        ValidationResult error = ValidationResult.Error("fail");

        Assert.True(success);
        Assert.False(error);
    }

    [Fact]
    public void ValidationResult_ToBoolean()
    {
        Assert.True(ValidationResult.Success.ToBoolean());
        Assert.False(ValidationResult.Error("fail").ToBoolean());
    }

    #endregion

    #region TopicNameValidator Tests

    [Theory]
    [InlineData("my-topic")]
    [InlineData("topic_name")]
    [InlineData("topic.name")]
    [InlineData("a")]
    [InlineData("topic-with-dashes-and-dots.v1")]
    [InlineData("UPPERCASE")]
    [InlineData("MixedCase")]
    [InlineData("topic123")]
    [InlineData("123")]
    public void TopicNameValidator_ValidNames(string name)
    {
        var result = TopicNameValidator.Validate(name);
        Assert.True(result.IsValid);
        Assert.True(TopicNameValidator.IsValid(name));
    }

    [Fact]
    public void TopicNameValidator_Null_Invalid()
    {
        var result = TopicNameValidator.Validate(null);
        Assert.False(result.IsValid);
        Assert.Contains("null or empty", result.ErrorMessage!);
    }

    [Fact]
    public void TopicNameValidator_Empty_Invalid()
    {
        var result = TopicNameValidator.Validate("");
        Assert.False(result.IsValid);
        Assert.Contains("null or empty", result.ErrorMessage!);
    }

    [Fact]
    public void TopicNameValidator_Dot_Invalid()
    {
        var result = TopicNameValidator.Validate(".");
        Assert.False(result.IsValid);
        Assert.Contains("cannot be '.' or '..'", result.ErrorMessage!);
    }

    [Fact]
    public void TopicNameValidator_DoubleDot_Invalid()
    {
        var result = TopicNameValidator.Validate("..");
        Assert.False(result.IsValid);
        Assert.Contains("cannot be '.' or '..'", result.ErrorMessage!);
    }

    [Theory]
    [InlineData("topic with spaces")]
    [InlineData("topic/slash")]
    [InlineData("topic@at")]
    [InlineData("topic#hash")]
    [InlineData("topic$dollar")]
    public void TopicNameValidator_InvalidCharacters(string name)
    {
        var result = TopicNameValidator.Validate(name);
        Assert.False(result.IsValid);
        Assert.Contains("alphanumeric", result.ErrorMessage!);
    }

    [Fact]
    public void TopicNameValidator_TooLong_Invalid()
    {
        var longName = new string('a', TopicNameValidator.MaxTopicNameLength + 1);
        var result = TopicNameValidator.Validate(longName);
        Assert.False(result.IsValid);
        Assert.Contains("exceed", result.ErrorMessage!);
    }

    [Fact]
    public void TopicNameValidator_MaxLength_Valid()
    {
        var name = new string('a', TopicNameValidator.MaxTopicNameLength);
        Assert.True(TopicNameValidator.IsValid(name));
    }

    [Fact]
    public void TopicNameValidator_MaxLengthConstant()
    {
        Assert.Equal(249, TopicNameValidator.MaxTopicNameLength);
    }

    #endregion

    #region BootstrapServerValidator Tests

    [Theory]
    [InlineData("localhost:9092")]
    [InlineData("broker.example.com:9093")]
    [InlineData("192.168.1.1:9092")]
    [InlineData("localhost")]
    [InlineData("[::1]:9092")]
    public void BootstrapServerValidator_ValidServers(string server)
    {
        var result = BootstrapServerValidator.Validate(server);
        Assert.True(result.IsValid, $"Expected '{server}' to be valid but got: {result.ErrorMessage}");
        Assert.True(BootstrapServerValidator.IsValid(server));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void BootstrapServerValidator_NullOrEmpty_Invalid(string? server)
    {
        var result = BootstrapServerValidator.Validate(server);
        Assert.False(result.IsValid);
        Assert.Contains("null or empty", result.ErrorMessage!);
    }

    [Fact]
    public void BootstrapServerValidator_InvalidPort_NonNumeric()
    {
        var result = BootstrapServerValidator.Validate("localhost:abc");
        Assert.False(result.IsValid);
        Assert.Contains("port must be a number", result.ErrorMessage!);
    }

    [Fact]
    public void BootstrapServerValidator_InvalidPort_Zero()
    {
        var result = BootstrapServerValidator.Validate("localhost:0");
        Assert.False(result.IsValid);
        Assert.Contains("port must be between", result.ErrorMessage!);
    }

    [Fact]
    public void BootstrapServerValidator_InvalidPort_TooHigh()
    {
        var result = BootstrapServerValidator.Validate("localhost:99999");
        Assert.False(result.IsValid);
        Assert.Contains("port must be between", result.ErrorMessage!);
    }

    [Fact]
    public void BootstrapServerValidator_ValidPort_Boundaries()
    {
        Assert.True(BootstrapServerValidator.IsValid("localhost:1"));
        Assert.True(BootstrapServerValidator.IsValid("localhost:65535"));
    }

    [Fact]
    public void BootstrapServerValidator_IPv6_MissingClosingBracket()
    {
        var result = BootstrapServerValidator.Validate("[::1");
        Assert.False(result.IsValid);
        Assert.Contains("missing closing bracket", result.ErrorMessage!);
    }

    #endregion

    #region Guard Tests

    [Fact]
    public void Guard_NotNullOrEmpty_Null_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.NotNullOrEmpty(null));
    }

    [Fact]
    public void Guard_NotNullOrEmpty_Empty_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.NotNullOrEmpty(""));
    }

    [Fact]
    public void Guard_NotNullOrEmpty_Whitespace_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.NotNullOrEmpty("   "));
    }

    [Fact]
    public void Guard_NotNullOrEmpty_Valid_DoesNotThrow()
    {
        Guard.NotNullOrEmpty("value");
    }

    [Fact]
    public void Guard_ValidTopicName_Null_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.ValidTopicName(null));
    }

    [Fact]
    public void Guard_ValidTopicName_InvalidChars_Throws()
    {
        var ex = Assert.Throws<InvalidConfigurationException>(() => Guard.ValidTopicName("topic/invalid"));
        Assert.Contains("alphanumeric", ex.Reason!);
    }

    [Fact]
    public void Guard_ValidTopicName_Valid_DoesNotThrow()
    {
        Guard.ValidTopicName("valid-topic");
    }

    [Fact]
    public void Guard_ValidPartition_Negative_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.ValidPartition(-1));
    }

    [Fact]
    public void Guard_ValidPartition_Zero_DoesNotThrow()
    {
        Guard.ValidPartition(0);
    }

    [Fact]
    public void Guard_ValidPartition_WithMaxPartitions_ExceedsMax_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.ValidPartition(5, maxPartitions: 5));
    }

    [Fact]
    public void Guard_ValidPartition_WithMaxPartitions_AtMax_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.ValidPartition(3, maxPartitions: 3));
    }

    [Fact]
    public void Guard_ValidPartition_WithMaxPartitions_BelowMax_DoesNotThrow()
    {
        Guard.ValidPartition(2, maxPartitions: 3);
    }

    [Fact]
    public void Guard_ValidTimeout_Zero_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.ValidTimeout(TimeSpan.Zero));
    }

    [Fact]
    public void Guard_ValidTimeout_Negative_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.ValidTimeout(TimeSpan.FromSeconds(-1)));
    }

    [Fact]
    public void Guard_ValidTimeout_TooLarge_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(
            () => Guard.ValidTimeout(TimeSpan.FromMinutes(15), TimeSpan.FromMinutes(10)));
    }

    [Fact]
    public void Guard_ValidTimeout_Valid_DoesNotThrow()
    {
        Guard.ValidTimeout(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void Guard_ValidTimeoutMs_Zero_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.ValidTimeoutMs(0));
    }

    [Fact]
    public void Guard_ValidTimeoutMs_Negative_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.ValidTimeoutMs(-1));
    }

    [Fact]
    public void Guard_ValidTimeoutMs_TooLarge_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.ValidTimeoutMs(700000));
    }

    [Fact]
    public void Guard_ValidTimeoutMs_Valid_DoesNotThrow()
    {
        Guard.ValidTimeoutMs(5000);
    }

    [Fact]
    public void Guard_ValidBootstrapServers_Null_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.ValidBootstrapServers(null));
    }

    [Fact]
    public void Guard_ValidBootstrapServers_Empty_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.ValidBootstrapServers(""));
    }

    [Fact]
    public void Guard_ValidBootstrapServers_Valid_DoesNotThrow()
    {
        Guard.ValidBootstrapServers("localhost:9092");
    }

    [Fact]
    public void Guard_ValidBootstrapServers_MultipleValid_DoesNotThrow()
    {
        Guard.ValidBootstrapServers("broker1:9092,broker2:9093");
    }

    [Fact]
    public void Guard_InRange_WithinRange_DoesNotThrow()
    {
        Guard.InRange(5, 1, 10);
    }

    [Fact]
    public void Guard_InRange_AtBoundaries_DoesNotThrow()
    {
        Guard.InRange(1, 1, 10);
        Guard.InRange(10, 1, 10);
    }

    [Fact]
    public void Guard_InRange_BelowMin_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.InRange(0, 1, 10));
    }

    [Fact]
    public void Guard_InRange_AboveMax_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.InRange(11, 1, 10));
    }

    [Fact]
    public void Guard_GreaterThan_Equal_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.GreaterThan(5, 5));
    }

    [Fact]
    public void Guard_GreaterThan_Less_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.GreaterThan(4, 5));
    }

    [Fact]
    public void Guard_GreaterThan_Greater_DoesNotThrow()
    {
        Guard.GreaterThan(6, 5);
    }

    [Fact]
    public void Guard_GreaterThanOrEqual_Equal_DoesNotThrow()
    {
        Guard.GreaterThanOrEqual(5, 5);
    }

    [Fact]
    public void Guard_GreaterThanOrEqual_Less_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.GreaterThanOrEqual(4, 5));
    }

    [Fact]
    public void Guard_ValidClientId_Null_DoesNotThrow()
    {
        Guard.ValidClientId(null); // ClientId is optional
    }

    [Fact]
    public void Guard_ValidClientId_TooLong_Throws()
    {
        var longId = new string('a', 256);
        Assert.Throws<InvalidConfigurationException>(() => Guard.ValidClientId(longId));
    }

    [Fact]
    public void Guard_ValidClientId_MaxLength_DoesNotThrow()
    {
        var maxId = new string('a', 255);
        Guard.ValidClientId(maxId);
    }

    [Fact]
    public void Guard_ValidGroupId_Null_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.ValidGroupId(null));
    }

    [Fact]
    public void Guard_ValidGroupId_Empty_Throws()
    {
        Assert.Throws<InvalidConfigurationException>(() => Guard.ValidGroupId(""));
    }

    [Fact]
    public void Guard_ValidGroupId_TooLong_Throws()
    {
        var longId = new string('g', 256);
        Assert.Throws<InvalidConfigurationException>(() => Guard.ValidGroupId(longId));
    }

    [Fact]
    public void Guard_ValidGroupId_Valid_DoesNotThrow()
    {
        Guard.ValidGroupId("my-consumer-group");
    }

    #endregion
}
