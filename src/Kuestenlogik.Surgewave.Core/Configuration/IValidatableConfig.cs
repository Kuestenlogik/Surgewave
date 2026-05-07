namespace Kuestenlogik.Surgewave.Core.Configuration;

/// <summary>
/// Marker interface for Surgewave configuration objects that support self-validation.
/// Implementations should call <see cref="ConfigValidator.ValidateDataAnnotations"/> to cover the
/// declarative constraints, then add their own cross-property checks.
///
/// <para>
/// The interface deliberately returns a <see cref="IReadOnlyList{T}"/> of error messages rather
/// than throwing — callers can choose whether to throw, log, or surface the errors in a UI. Use
/// <see cref="ConfigValidator.ThrowIfInvalid"/> for the fail-fast path.
/// </para>
/// </summary>
public interface IValidatableConfig
{
    /// <summary>
    /// Runs all validation rules and returns the set of error messages, or an empty list when the
    /// configuration is valid.
    /// </summary>
    IReadOnlyList<string> Validate();
}
