using System.Text.Json;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Link;

/// <summary>
/// Shared helpers for cluster link commands talking to the broker REST API
/// (/api/cluster-links).
/// </summary>
internal static class LinkApi
{
    /// <summary>
    /// Extracts a human-readable error message from an error response body
    /// ({"message": "..."} or {"error": "..."}), falling back to the raw body.
    /// </summary>
    public static string ExtractErrorMessage(string body)
    {
        if (string.IsNullOrWhiteSpace(body))
        {
            return "(no details)";
        }

        try
        {
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            if (root.ValueKind == JsonValueKind.Object)
            {
                if (root.TryGetProperty("message", out var message) && message.ValueKind == JsonValueKind.String)
                {
                    return message.GetString()!;
                }
                if (root.TryGetProperty("error", out var error) && error.ValueKind == JsonValueKind.String)
                {
                    return error.GetString()!;
                }
            }
        }
        catch (JsonException)
        {
            // Not JSON — return raw body below
        }

        return body;
    }

    public static string? GetString(JsonElement element, string name)
        => element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    public static long GetInt64(JsonElement element, string name)
        => element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number
            ? value.GetInt64()
            : 0;

    public static string StateColor(string? state) => state?.ToUpperInvariant() switch
    {
        "ACTIVE" => "green",
        "PAUSED" => "yellow",
        "ERROR" => "red",
        _ => "blue"
    };

    /// <summary>
    /// Renders a link status DTO ({linkId, state, remoteClusterId,
    /// mirroredTopicCount, totalLag, lastFetch}) as a property grid.
    /// </summary>
    public static void WriteStatusGrid(JsonElement status)
    {
        var state = GetString(status, "state") ?? "unknown";

        var grid = new Grid();
        grid.AddColumn();
        grid.AddColumn();
        grid.AddRow("[bold]Link ID:[/]", Markup.Escape(GetString(status, "linkId") ?? ""));
        grid.AddRow("[bold]State:[/]", $"[{StateColor(state)}]{Markup.Escape(state)}[/]");
        grid.AddRow("[bold]Remote Cluster:[/]", Markup.Escape(GetString(status, "remoteClusterId") ?? "unknown"));
        grid.AddRow("[bold]Mirrored Topics:[/]", GetInt64(status, "mirroredTopicCount").ToString());
        grid.AddRow("[bold]Total Lag:[/]", $"{GetInt64(status, "totalLag")} messages");
        grid.AddRow("[bold]Last Fetch:[/]", Markup.Escape(GetString(status, "lastFetch") ?? "never"));
        AnsiConsole.Write(grid);
    }
}
