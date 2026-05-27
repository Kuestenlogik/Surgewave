using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Transactions;

internal static class TransactionsJsonOptions
{
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };
}

/// <summary>
/// Command for transaction operations (surgewave transactions ...)
/// </summary>
public class TransactionsCommand : CommandBase
{
    public TransactionsCommand() : base("transactions", "Transaction management operations")
    {
        Aliases.Add("txn");
        Subcommands.Add(new ListTransactionsCommand());
        Subcommands.Add(new DescribeTransactionCommand());
    }
}

/// <summary>
/// List transactions (surgewave transactions list)
/// </summary>
public class ListTransactionsCommand : CommandBase
{
    public ListTransactionsCommand() : base("list", "List active transactions")
    {
        Aliases.Add("ls");
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var transactions = await client.Transactions.ListAsync(ct);

            if (format == OutputFormat.Json)
            {
                var output = transactions.Select(t => new
                {
                    t.TransactionalId,
                    t.State,
                    t.ProducerId,
                    t.ProducerEpoch
                });
                Console.WriteLine(JsonSerializer.Serialize(output, TransactionsJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var txn in transactions)
                {
                    Console.WriteLine($"{txn.TransactionalId}\t{txn.State}\t{txn.ProducerId}\t{txn.ProducerEpoch}");
                }
            }
            else
            {
                if (transactions.Count == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No active transactions.[/]");
                    return 0;
                }

                AnsiConsole.Write(new Rule("[bold blue]Active Transactions[/]").LeftJustified());
                AnsiConsole.WriteLine();

                var table = new Table();
                table.AddColumn("Transactional ID");
                table.AddColumn("State");
                table.AddColumn("Producer ID");
                table.AddColumn("Epoch");

                foreach (var txn in transactions)
                {
                    var stateColor = txn.State switch
                    {
                        "Ongoing" => "yellow",
                        "PrepareCommit" => "blue",
                        "PrepareAbort" => "red",
                        "CompleteCommit" => "green",
                        "CompleteAbort" => "dim",
                        _ => "white"
                    };

                    table.AddRow(
                        $"[cyan]{txn.TransactionalId}[/]",
                        $"[{stateColor}]{txn.State}[/]",
                        txn.ProducerId.ToString(),
                        txn.ProducerEpoch.ToString());
                }

                AnsiConsole.Write(table);
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine($"[dim]Total transactions: {transactions.Count}[/]");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to list transactions: {ex.Message}");
            return 1;
        }
    }
}

/// <summary>
/// Describe a transaction (surgewave transactions describe)
/// </summary>
public class DescribeTransactionCommand : CommandBase
{
    private readonly Argument<string> _txnIdArg = new("transactional-id") { Description = "The transactional ID to describe" };

    public DescribeTransactionCommand() : base("describe", "Describe a transaction")
    {
        Aliases.Add("show");
        Arguments.Add(_txnIdArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var txnId = parseResult.GetValue(_txnIdArg);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var descriptions = await client.Transactions.DescribeAsync([txnId], ct);

            if (descriptions.Count == 0)
            {
                WriteError($"Transaction '{txnId}' not found");
                return 1;
            }

            var txn = descriptions[0];

            if (format == OutputFormat.Json)
            {
                var output = new
                {
                    txn.TransactionalId,
                    txn.State,
                    txn.ProducerId,
                    txn.ProducerEpoch,
                    Partitions = txn.Partitions.Select(p => new { p.Topic, p.Partition })
                };
                Console.WriteLine(JsonSerializer.Serialize(output, TransactionsJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"TransactionalId: {txn.TransactionalId}");
                Console.WriteLine($"State: {txn.State}");
                Console.WriteLine($"ProducerId: {txn.ProducerId}");
                Console.WriteLine($"ProducerEpoch: {txn.ProducerEpoch}");
                Console.WriteLine($"Partitions: {string.Join(", ", txn.Partitions.Select(p => $"{p.Topic}-{p.Partition}"))}");
            }
            else
            {
                AnsiConsole.Write(new Rule($"[bold blue]Transaction: {txn.TransactionalId}[/]").LeftJustified());
                AnsiConsole.WriteLine();

                var grid = new Grid();
                grid.AddColumn();
                grid.AddColumn();

                var stateColor = txn.State switch
                {
                    "Ongoing" => "yellow",
                    "PrepareCommit" => "blue",
                    "PrepareAbort" => "red",
                    "CompleteCommit" => "green",
                    "CompleteAbort" => "dim",
                    _ => "white"
                };

                grid.AddRow("[bold]State:[/]", $"[{stateColor}]{txn.State}[/]");
                grid.AddRow("[bold]Producer ID:[/]", txn.ProducerId.ToString());
                grid.AddRow("[bold]Producer Epoch:[/]", txn.ProducerEpoch.ToString());

                AnsiConsole.Write(grid);
                AnsiConsole.WriteLine();

                if (txn.Partitions.Count > 0)
                {
                    AnsiConsole.MarkupLine("[bold]Partitions in transaction:[/]");
                    var partitionTable = new Table();
                    partitionTable.AddColumn("Topic");
                    partitionTable.AddColumn("Partition");

                    foreach (var (topic, partition) in txn.Partitions)
                    {
                        partitionTable.AddRow($"[cyan]{topic}[/]", partition.ToString());
                    }

                    AnsiConsole.Write(partitionTable);
                }
                else
                {
                    AnsiConsole.MarkupLine("[dim]No partitions in transaction.[/]");
                }
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to describe transaction: {ex.Message}");
            return 1;
        }
    }
}
