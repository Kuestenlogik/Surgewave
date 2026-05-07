using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Resolves ${variable} syntax in pipeline configuration values.
/// Built-in variables: pipeline.id, pipeline.name, node.id, timestamp, timestamp.epoch, date, env.*.
/// User-defined: param.* or direct key lookup from Parameters dictionary.
/// </summary>
public static partial class PipelineVariableResolver
{
    /// <summary>
    /// Resolve all variables in a configuration dictionary.
    /// </summary>
    public static Dictionary<string, string> Resolve(
        Dictionary<string, string> config,
        PipelineVariableContext context,
        ILogger? logger = null)
    {
        var resolved = new Dictionary<string, string>(config.Count);

        foreach (var (key, value) in config)
        {
            resolved[key] = ResolveValue(value, context, logger);
        }

        return resolved;
    }

    /// <summary>
    /// Resolve variables in a single string value.
    /// </summary>
    public static string ResolveValue(string value, PipelineVariableContext context, ILogger? logger = null)
    {
        if (string.IsNullOrEmpty(value) || !value.Contains("${", StringComparison.Ordinal))
            return value;

        return VariablePattern().Replace(value, match =>
        {
            var varName = match.Groups[1].Value;
            var resolved = ResolveVariable(varName, context);

            if (resolved is null)
            {
                logger?.LogWarning("Unresolved pipeline variable: ${{{Variable}}}", varName);
                return match.Value; // Keep original text
            }

            return resolved;
        });
    }

    private static string? ResolveVariable(string name, PipelineVariableContext context)
    {
        // Built-in variables
        switch (name)
        {
            case "pipeline.id":
                return context.PipelineId;
            case "pipeline.name":
                return context.PipelineName;
            case "node.id":
                return context.NodeId ?? "";
            case "timestamp":
                return DateTimeOffset.UtcNow.ToString("o");
            case "timestamp.epoch":
                return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();
            case "date":
                return DateTime.UtcNow.ToString("yyyy-MM-dd");
        }

        // Environment variables: ${env.VAR_NAME}
        if (name.StartsWith("env.", StringComparison.Ordinal))
        {
            var envName = name[4..];
            return Environment.GetEnvironmentVariable(envName) ?? "";
        }

        // User parameters: ${param.key} or direct key lookup
        if (name.StartsWith("param.", StringComparison.Ordinal))
        {
            var paramKey = name[6..];
            return context.Parameters.TryGetValue(paramKey, out var paramValue) ? paramValue : null;
        }

        // Direct lookup in Parameters dictionary
        return context.Parameters.TryGetValue(name, out var value) ? value : null;
    }

    [GeneratedRegex(@"\$\{([^}]+)\}")]
    private static partial Regex VariablePattern();
}
