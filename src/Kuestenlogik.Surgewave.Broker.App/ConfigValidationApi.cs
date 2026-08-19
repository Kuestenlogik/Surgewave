using System.Reflection;
using Kuestenlogik.Surgewave.Core.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Live config validation as a Surgewave Control / operations endpoint.
///
/// <para>
/// The CLI <c>surgewave config validate</c> command parses an <c>appsettings.json</c> file
/// off disk, layers in <c>pluginsettings.json</c> defaults, and runs every discovered
/// <see cref="IValidatableConfig"/> type's <c>Validate()</c> method against the merged
/// JSON. That works for pre-flight checks but cannot answer the operationally interesting
/// question: <em>does the config the broker is actually running on still validate?</em>
/// Environment-variable overrides, plugin defaults loaded from a path that the CLI cannot
/// see, command-line flags, and runtime config mutations all bypass the file.
/// </para>
///
/// <para>
/// <c>GET /api/config/validate</c> answers that question. It walks the broker's live
/// <see cref="IConfiguration"/> directly, discovers every concrete <c>IValidatableConfig</c>
/// type in the loaded assemblies (broker + protocol plugins + broker plugins), reads each
/// type's <c>SectionName</c> constant, binds the matching <c>IConfigurationSection</c> onto
/// a fresh instance, and runs <c>Validate()</c>. The response shape mirrors the CLI's
/// JSON output for parity, so the same Surgewave Control UI panel can render either source.
/// </para>
///
/// <para>
/// HTTP status codes: <c>200</c> if every present section validates clean, <c>422</c>
/// if at least one section has errors. Both responses carry the same body shape so a
/// dashboard can unconditionally render the per-section result list.
/// </para>
/// </summary>
public static class ConfigValidationApi
{
    /// <summary>
    /// Maps the live config validation endpoint onto <paramref name="app"/>:
    /// <c>GET /api/config/validate</c>.
    /// </summary>
    public static void MapConfigValidation(this WebApplication app)
    {
        app.MapGet("/api/config/validate", (
            IConfiguration configuration,
            ILoggerFactory loggerFactory) =>
        {
            var logger = loggerFactory.CreateLogger("ConfigValidationApi");
            var checks = ValidateAll(configuration, logger);
            var failing = checks.Count(c => c.Errors.Count > 0);
            var statusCode = failing > 0 ? StatusCodes.Status422UnprocessableEntity : StatusCodes.Status200OK;

            return Results.Json(new ConfigValidationResponse
            {
                Total = checks.Count,
                Passed = checks.Count - failing,
                Failed = failing,
                Sections = checks,
            }, statusCode: statusCode);
        }).WithName("ValidateLiveConfiguration");
    }

    private static IReadOnlyList<SectionResult> ValidateAll(IConfiguration configuration, ILogger logger)
    {
        var results = new List<SectionResult>();
        foreach (var type in DiscoverValidatableConfigTypes())
        {
            var sectionName = ReadSectionName(type);
            if (sectionName is null) continue;

            var section = configuration.GetSection(sectionName);
            if (!section.Exists()) continue;

            try
            {
                var instance = (IValidatableConfig?)Activator.CreateInstance(type);
                if (instance is null)
                {
                    results.Add(new SectionResult(sectionName, type.Name, "fail", ["Type has no parameterless constructor"]));
                    continue;
                }
                section.Bind(instance);
                var errors = instance.Validate();
                results.Add(new SectionResult(
                    sectionName,
                    type.Name,
                    errors.Count == 0 ? "ok" : "fail",
                    errors));
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to validate config section {Section} ({Type})", sectionName, type.Name);
                results.Add(new SectionResult(sectionName, type.Name, "fail", [$"Bind/Validate threw: {ex.Message}"]));
            }
        }
        return results;
    }

    private static IEnumerable<Type> DiscoverValidatableConfigTypes()
    {
        // Walk every loaded assembly that looks like a Surgewave assembly. Plugin DLLs are
        // already loaded by this point in the broker's lifecycle (BrokerPluginActivator
        // ran earlier), so plugin-defined IValidatableConfig types are picked up too.
        foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
        {
            var name = asm.GetName().Name;
            if (name is null) continue;
            if (!name.StartsWith("Kuestenlogik.Surgewave.", StringComparison.Ordinal)) continue;

            Type?[] types;
            try
            {
                types = asm.GetTypes();
            }
            catch (ReflectionTypeLoadException ex)
            {
                types = ex.Types;
            }
            catch
            {
                continue;
            }

            foreach (var type in types)
            {
                if (type is null) continue;
                if (type.IsAbstract || type.IsInterface) continue;
                if (!typeof(IValidatableConfig).IsAssignableFrom(type)) continue;
                if (type.GetConstructor(Type.EmptyTypes) is null) continue;
                yield return type;
            }
        }
    }

    private static string? ReadSectionName(Type type)
    {
        var field = type.GetField("SectionName", BindingFlags.Public | BindingFlags.Static);
        if (field is null || field.FieldType != typeof(string)) return null;
        return field.GetRawConstantValue() as string ?? field.GetValue(null) as string;
    }

    public sealed record SectionResult(
        string Section,
        string Class,
        string Status,
        IReadOnlyList<string> Errors);

    public sealed class ConfigValidationResponse
    {
        public int Total { get; init; }
        public int Passed { get; init; }
        public int Failed { get; init; }
        public IReadOnlyList<SectionResult> Sections { get; init; } = [];
    }
}
