namespace Kuestenlogik.Surgewave.Client.Validation;

/// <summary>
/// Represents the result of a validation operation.
/// </summary>
public readonly struct ValidationResult
{
    /// <summary>
    /// A successful validation result.
    /// </summary>
    public static readonly ValidationResult Success = new(true, null);

    /// <summary>
    /// Whether the validation succeeded.
    /// </summary>
    public bool IsValid { get; }

    /// <summary>
    /// The error message if validation failed.
    /// </summary>
    public string? ErrorMessage { get; }

    private ValidationResult(bool isValid, string? errorMessage)
    {
        IsValid = isValid;
        ErrorMessage = errorMessage;
    }

    /// <summary>
    /// Creates a failed validation result with the specified error message.
    /// </summary>
    public static ValidationResult Error(string message) => new(false, message);

    /// <summary>
    /// Converts the validation result to a boolean.
    /// </summary>
    public bool ToBoolean() => IsValid;

    /// <summary>
    /// Implicitly converts a ValidationResult to a boolean.
    /// </summary>
    public static implicit operator bool(ValidationResult result) => result.IsValid;
}
