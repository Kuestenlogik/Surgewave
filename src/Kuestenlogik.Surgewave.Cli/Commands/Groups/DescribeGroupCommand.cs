using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Groups;

/// <summary>
/// Describe a consumer group (surgewave groups describe)
/// </summary>
public class DescribeGroupCommand : CommandBase
{
    private readonly Argument<string> _groupArg = new("group") { Description = "Consumer group ID" };

    public DescribeGroupCommand() : base("describe", "Describe a consumer group")
    {
        Arguments.Add(_groupArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var groupId = parseResult.GetValue(_groupArg);
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var group = await client.Groups.DescribeAsync(groupId, ct);

            if (group.ErrorCode != 0)
            {
                WriteError($"Group '{groupId}' not found (error code: {group.ErrorCode})");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                var output = new
                {
                    group.GroupId,
                    group.State,
                    group.ProtocolType,
                    group.ProtocolName,
                    group.GenerationId,
                    MemberCount = group.Members.Count,
                    Members = group.Members.Select(m => new
                    {
                        m.MemberId,
                        m.GroupInstanceId,
                        m.ClientId
                    })
                };
                Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"GroupId: {group.GroupId}");
                Console.WriteLine($"State: {group.State}");
                Console.WriteLine($"ProtocolType: {group.ProtocolType}");
                Console.WriteLine($"ProtocolName: {group.ProtocolName}");
                Console.WriteLine($"GenerationId: {group.GenerationId}");
                Console.WriteLine($"Members: {group.Members.Count}");
                foreach (var member in group.Members)
                {
                    Console.WriteLine($"  - {member.MemberId} ({member.ClientId})");
                }
            }
            else
            {
                var panel = new Panel(new Markup($"""
                    [bold]State:[/] {GetStateMarkup(group.State)}
                    [bold]Protocol Type:[/] {group.ProtocolType}
                    [bold]Protocol Name:[/] {group.ProtocolName}
                    [bold]Generation ID:[/] {group.GenerationId}
                    [bold]Members:[/] {group.Members.Count}
                    """))
                {
                    Header = new PanelHeader($"[cyan]{group.GroupId}[/]")
                };
                AnsiConsole.Write(panel);

                if (group.Members.Count > 0)
                {
                    AnsiConsole.WriteLine();
                    var table = new Table();
                    table.Title = new TableTitle("[bold]Members[/]");
                    table.AddColumn("Member ID");
                    table.AddColumn("Client ID");
                    table.AddColumn("Instance ID");

                    foreach (var member in group.Members)
                    {
                        table.AddRow(
                            $"[dim]{member.MemberId[..Math.Min(40, member.MemberId.Length)]}...[/]",
                            member.ClientId,
                            member.GroupInstanceId ?? "[dim]N/A[/]");
                    }

                    AnsiConsole.Write(table);
                }
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to describe group: {ex.Message}");
            return 1;
        }
    }

    private static string GetStateMarkup(string state) => state switch
    {
        "Stable" => "[green]Stable[/]",
        "Empty" => "[dim]Empty[/]",
        "PreparingRebalance" => "[yellow]PreparingRebalance[/]",
        "CompletingRebalance" => "[yellow]CompletingRebalance[/]",
        "Dead" => "[red]Dead[/]",
        _ => state
    };
}
