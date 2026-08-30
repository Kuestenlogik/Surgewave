using System.ComponentModel.DataAnnotations;

namespace Kuestenlogik.Surgewave.Core.Configuration;

/// <summary>
/// Reusable helpers for validating Surgewave configuration objects.
///
/// <para>
/// Surgewave configs come in two flavours: loaded via <c>IOptions&lt;T&gt;</c> from configuration files
/// (where <see cref="Microsoft.Extensions.Options.IValidateOptions{TOptions}"/> is the idiomatic
/// validation entry point), and directly instantiated via <c>new XConfig { ... }</c> in samples,
/// tests, and programmatic setups (where <c>IValidateOptions</c> never runs). This helper lets a
/// single <see cref="IValidatableConfig.Validate"/> implementation cover both worlds: it can be
/// invoked from an <c>IValidateOptions</c> validator in DI-land, and by user code after
/// construction for direct-instantiated configs.
/// </para>
/// </summary>
public static class ConfigValidator
{
    /// <summary>
    /// Evaluates all <see cref="ValidationAttribute"/>s declared on the properties of
    /// <paramref name="config"/> and returns any error messages.
    /// </summary>
    /// <param name="config">The configuration instance to validate.</param>
    /// <returns>Human-readable error messages, one per failed constraint; empty when valid.</returns>
    public static IReadOnlyList<string> ValidateDataAnnotations(object config)
    {
        ArgumentNullException.ThrowIfNull(config);

        var context = new ValidationContext(config);
        var results = new List<ValidationResult>();
        Validator.TryValidateObject(config, context, results, validateAllProperties: true);

        if (results.Count == 0) return Array.Empty<string>();

        var messages = new List<string>(results.Count);
        foreach (var result in results)
        {
            var member = result.MemberNames.FirstOrDefault();
            var message = result.ErrorMessage ?? "Validation failed.";
            messages.Add(string.IsNullOrEmpty(member) ? message : $"{member}: {message}");
        }
        return messages;
    }

    /// <summary>
    /// Runs <see cref="IValidatableConfig.Validate"/> and throws
    /// <see cref="ConfigValidationException"/> with the aggregated messages if any errors exist.
    /// Use this for fail-fast validation at application startup.
    /// </summary>
    public static void ThrowIfInvalid(IValidatableConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        var errors = config.Validate();
        if (errors.Count > 0)
        {
            throw new ConfigValidationException(config.GetType(), errors);
        }
    }

    /// <summary>
    /// Runs <see cref="IValidatableConfig.Validate"/> on every config and throws once with
    /// everything that is wrong (#170).
    /// </summary>
    /// <remarks>
    /// <para>
    /// One exception for all of them, not one per config: an operator fixing a deployment file
    /// should see every mistake in it, not the first one and then the next after another restart.
    /// </para>
    /// <para>
    /// Nulls are skipped, so a caller can pass a config that only exists when a feature is on
    /// without branching at the call site.
    /// </para>
    /// </remarks>
    public static void ThrowIfAnyInvalid(params IValidatableConfig?[] configs)
    {
        ArgumentNullException.ThrowIfNull(configs);

        List<Type>? failedTypes = null;
        List<string>? allErrors = null;

        foreach (var config in configs)
        {
            if (config is null) continue;

            var errors = config.Validate();
            if (errors.Count == 0) continue;

            var type = config.GetType();
            (failedTypes ??= []).Add(type);
            (allErrors ??= []).AddRange(errors.Select(e => $"{type.Name}: {e}"));
        }

        if (failedTypes is not null)
        {
            throw new ConfigValidationException(failedTypes, allErrors!);
        }
    }

    /// <summary>
    /// Helper for configs that only need the standard DataAnnotations pass. Common shape:
    /// <code>
    /// public IReadOnlyList&lt;string&gt; Validate() =&gt; ConfigValidator.ValidateDataAnnotations(this);
    /// </code>
    /// Configs with cross-property rules should instead call <see cref="ValidateDataAnnotations"/>
    /// directly and append their own messages.
    /// </summary>
    public static IReadOnlyList<string> ValidateOnly<T>(T config) where T : IValidatableConfig
        => ValidateDataAnnotations(config!);
}

/// <summary>
/// Raised when a configuration fails validation at startup (i.e. via
/// <see cref="ConfigValidator.ThrowIfInvalid"/>).
/// </summary>
public sealed class ConfigValidationException : Exception
{
    /// <summary>
    /// The type of the configuration that failed validation — the first one when several did.
    /// </summary>
    public Type ConfigType => ConfigTypes[0];

    /// <summary>
    /// Every configuration type that failed, in the order they were checked.
    /// </summary>
    public IReadOnlyList<Type> ConfigTypes { get; }

    /// <summary>
    /// The individual error messages reported by the configs' <c>Validate()</c> implementations.
    /// </summary>
    public IReadOnlyList<string> Errors { get; }

    internal ConfigValidationException(Type configType, IReadOnlyList<string> errors)
        : this([configType], errors)
    {
    }

    internal ConfigValidationException(IReadOnlyList<Type> configTypes, IReadOnlyList<string> errors)
        : base(BuildMessage(configTypes, errors))
    {
        ConfigTypes = configTypes;
        Errors = errors;
    }

    private static string BuildMessage(IReadOnlyList<Type> configTypes, IReadOnlyList<string> errors)
    {
        var names = string.Join(", ", configTypes.Select(t => t.Name));
        var header = $"{names} failed validation with {errors.Count} error(s):";
        return string.Join(Environment.NewLine, new[] { header }.Concat(errors.Select(e => "  - " + e)));
    }
}
