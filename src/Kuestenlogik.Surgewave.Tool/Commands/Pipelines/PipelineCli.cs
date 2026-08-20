using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Connect.Pipelines;
using Kuestenlogik.Surgewave.Pipelines.Publishing;

namespace Kuestenlogik.Surgewave.Cli.Commands.Pipelines;

/// <summary>
/// Shared helpers for the pipelines command group.
/// </summary>
internal static class PipelineCli
{
    /// <summary>
    /// Creates a publisher for the broker admin endpoint from <c>--broker-url</c>
    /// (or SURGEWAVE_BROKER_URL, default https://localhost:9093).
    /// </summary>
    public static PipelinePublisher CreatePublisher(ParseResult parseResult)
    {
        var brokerUrl = parseResult.GetValue(GlobalOptions.BrokerUrl) ?? "https://localhost:9093";

        // "localhost:9093" parses as a URI with scheme "localhost" — catch that early
        // instead of failing with a raw HttpClient exception at send time.
        if (!Uri.TryCreate(brokerUrl, UriKind.Absolute, out var uri)
            || (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps))
        {
            throw new PipelinePublishException(
                $"'{brokerUrl}' is not a broker URL — expected http(s)://host:port (default https://localhost:9093).");
        }

        return new PipelinePublisher(uri);
    }

    /// <summary>Resolves a pipeline by id or name, or fails with a CLI error.</summary>
    public static async Task<PipelineDefinition?> ResolveAsync(
        PipelinePublisher publisher,
        string idOrName,
        CancellationToken ct)
    {
        return await publisher.FindAsync(idOrName, ct);
    }

    /// <summary>Human-readable pipeline status.</summary>
    public static string StatusText(PipelineStatus status)
    {
        return status switch
        {
            PipelineStatus.Draft => "draft",
            PipelineStatus.Running => "running",
            PipelineStatus.Stopped => "stopped",
            PipelineStatus.Failed => "failed",
            _ => status.ToString().ToLowerInvariant(),
        };
    }

    /// <summary>Spectre color markup for a pipeline status.</summary>
    public static string StatusMarkup(PipelineStatus status)
    {
        var text = StatusText(status);
        return status switch
        {
            PipelineStatus.Running => $"[green]{text}[/]",
            PipelineStatus.Failed => $"[red]{text}[/]",
            PipelineStatus.Stopped => $"[yellow]{text}[/]",
            _ => $"[grey]{text}[/]",
        };
    }
}
