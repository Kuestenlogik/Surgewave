using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Client.Validation;

/// <summary>
/// Validates topic names according to Kafka naming conventions.
/// </summary>
public static partial class TopicNameValidator
{
    /// <summary>
    /// Maximum length for a topic name.
    /// </summary>
    public const int MaxTopicNameLength = 249;

    /// <summary>
    /// Validates a topic name.
    /// </summary>
    /// <param name="topicName">The topic name to validate.</param>
    /// <returns>A validation result indicating success or the error message.</returns>
    public static ValidationResult Validate(string? topicName)
    {
        if (string.IsNullOrEmpty(topicName))
        {
            return ValidationResult.Error("topic name cannot be null or empty");
        }

        if (topicName.Length > MaxTopicNameLength)
        {
            return ValidationResult.Error($"topic name cannot exceed {MaxTopicNameLength} characters");
        }

        // Cannot be "." or ".."
        if (topicName is "." or "..")
        {
            return ValidationResult.Error("topic name cannot be '.' or '..'");
        }

        // Must contain only valid characters: alphanumeric, '.', '_', '-'
        if (!ValidTopicNameRegex().IsMatch(topicName))
        {
            return ValidationResult.Error(
                "topic name can only contain alphanumeric characters, '.', '_', and '-'");
        }

        return ValidationResult.Success;
    }

    /// <summary>
    /// Returns true if the topic name is valid.
    /// </summary>
    public static bool IsValid(string? topicName) => Validate(topicName).IsValid;

    [GeneratedRegex(@"^[a-zA-Z0-9._-]+$", RegexOptions.Compiled)]
    private static partial Regex ValidTopicNameRegex();
}
