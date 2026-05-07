using Kuestenlogik.Surgewave.Client.Diagnostics;

namespace Kuestenlogik.Surgewave.Client;

/// <summary>
/// Exception thrown when configuration is invalid.
/// </summary>
public class InvalidConfigurationException : SurgewaveClientException, IRecoverableException
{
    /// <summary>
    /// The name of the property with invalid configuration.
    /// </summary>
    public string PropertyName { get; }

    /// <summary>
    /// The invalid value that was provided.
    /// </summary>
    public object? InvalidValue { get; }

    /// <summary>
    /// A description of what makes the configuration invalid.
    /// </summary>
    public string? Reason { get; }

    /// <summary>
    /// Gets a suggestion for how to recover from this error.
    /// </summary>
    public string? RecoverySuggestion => Diagnostics.RecoverySuggestion.ForConfigurationError(PropertyName);

    public InvalidConfigurationException() : this("Configuration") { }

    public InvalidConfigurationException(string propertyName)
        : base($"Invalid configuration: {propertyName} is required")
    {
        PropertyName = propertyName;
    }

    public InvalidConfigurationException(string propertyName, object? invalidValue)
        : base(FormatMessage(propertyName, invalidValue, null))
    {
        PropertyName = propertyName;
        InvalidValue = invalidValue;
    }

    public InvalidConfigurationException(string propertyName, object? invalidValue, string? reason)
        : base(FormatMessage(propertyName, invalidValue, reason))
    {
        PropertyName = propertyName;
        InvalidValue = invalidValue;
        Reason = reason;
    }

    public InvalidConfigurationException(string message, Exception innerException)
        : base(message, innerException)
    {
        PropertyName = "Unknown";
    }

    private static string FormatMessage(string propertyName, object? invalidValue, string? reason)
    {
        if (invalidValue == null)
            return $"Invalid configuration: {propertyName} is required";

        var reasonSuffix = reason != null ? $": {reason}" : "";
        return $"Invalid configuration: {propertyName} = '{invalidValue}'{reasonSuffix}";
    }
}
