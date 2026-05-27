using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Chat;

/// <summary>
/// Interactive chat via Surgewave topics.
/// Produces user messages to an inbound topic, consumes agent responses from an outbound topic.
///
/// Usage: surgewave chat --in chat-in --out chat-out
/// </summary>
public sealed class ChatCommand : CommandBase
{
    private readonly Option<string> _inTopicOpt = new("--in", "-i")
    {
        Description = "Inbound topic (user messages → agent)",
        Arity = ArgumentArity.ExactlyOne
    };

    private readonly Option<string> _outTopicOpt = new("--out", "-o")
    {
        Description = "Outbound topic (agent responses → user)",
        Arity = ArgumentArity.ExactlyOne
    };

    private readonly Option<string?> _sessionOpt = new("--session", "-s")
    {
        Description = "Session ID (auto-generated if omitted)"
    };

    public ChatCommand() : base("chat", "Interactive chat via Surgewave topics")
    {
        Options.Add(_inTopicOpt);
        Options.Add(_outTopicOpt);
        Options.Add(_sessionOpt);

        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var inTopic = parseResult.GetValue(_inTopicOpt)!;
        var outTopic = parseResult.GetValue(_outTopicOpt)!;
        var sessionId = parseResult.GetValue(_sessionOpt) ?? Guid.NewGuid().ToString("N")[..12];

        await using var client = new SurgewaveNativeClient(host, port);

        try
        {
            await client.ConnectAsync(ct);
        }
        catch (Exception ex)
        {
            WriteError($"Cannot connect to {host}:{port}: {ex.Message}");
            return 1;
        }

        Console.MarkupLine($"[cyan]Surgewave Chat[/] (Session: [dim]{sessionId}[/])");
        Console.MarkupLine($"[dim]{inTopic} → Agent → {outTopic}[/]");
        Console.MarkupLine("[dim]/quit to exit, /clear to reset[/]");
        Console.WriteLine();

        // Start consuming from outbound topic at current end (only see new messages)
        var offset = await GetEndOffsetAsync(client, outTopic, ct);

        while (!ct.IsCancellationRequested)
        {
            Console.Markup("[green]You:[/] ");
            var input = System.Console.ReadLine();

            if (input is null || ct.IsCancellationRequested)
                break;

            input = input.Trim();
            if (string.IsNullOrEmpty(input))
                continue;

            if (input.StartsWith('/'))
            {
                if (input is "/quit" or "/exit" or "/q")
                    break;
                if (input is "/clear")
                {
                    sessionId = Guid.NewGuid().ToString("N")[..12];
                    Console.MarkupLine($"[cyan]New session:[/] [dim]{sessionId}[/]");
                    continue;
                }
                if (input is "/session")
                {
                    Console.MarkupLine($"[dim]Session: {sessionId}[/]");
                    continue;
                }
                if (input is "/help")
                {
                    Console.MarkupLine("[dim]/quit    — Exit[/]");
                    Console.MarkupLine("[dim]/clear   — New session[/]");
                    Console.MarkupLine("[dim]/session — Show session ID[/]");
                    continue;
                }
                Console.MarkupLine($"[yellow]Unknown:[/] {Markup.Escape(input)}. /help for commands");
                continue;
            }

            // Send: produce to inbound topic, key = sessionId
            try
            {
                await client.Messaging.SendAsync(
                    inTopic, 0,
                    Encoding.UTF8.GetBytes(sessionId),
                    Encoding.UTF8.GetBytes(input),
                    ct);
            }
            catch (Exception ex)
            {
                Console.MarkupLine($"[red]Send error:[/] {Markup.Escape(ex.Message)}");
                continue;
            }

            // Receive: poll outbound topic for response
            var (response, newOffset) = await WaitForResponseAsync(client, outTopic, offset, ct);
            offset = newOffset;

            if (response is not null)
                Console.MarkupLine($"[blue]Agent:[/] {Markup.Escape(response)}");
            else
                Console.MarkupLine("[yellow]No response (timeout)[/]");

            Console.WriteLine();
        }

        Console.MarkupLine("[dim]Chat ended.[/]");
        return 0;
    }

    private static async Task<long> GetEndOffsetAsync(SurgewaveNativeClient client, string topic, CancellationToken ct)
    {
        try { return await client.Messaging.GetLatestOffsetAsync(topic, 0, ct); }
        catch { return 0; }
    }

    private static async Task<(string? response, long offset)> WaitForResponseAsync(
        SurgewaveNativeClient client, string outTopic, long offset, CancellationToken ct)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(60);

        while (DateTimeOffset.UtcNow < deadline && !ct.IsCancellationRequested)
        {
            var result = await client.Messaging.ReceiveAsync(
                outTopic, 0, offset, maxWaitMs: 2000, cancellationToken: ct);

            if (result.Messages.Count > 0)
            {
                var msg = result.Messages[^1];
                return (Encoding.UTF8.GetString(msg.Value), msg.Offset + 1);
            }

            await Task.Delay(100, ct);
        }

        return (null, offset);
    }
}
