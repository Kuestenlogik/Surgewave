using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Core.Tools;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Logs;

/// <summary>
/// Analyze log segment files (surgewave logs analyze)
/// Performs detailed RecordBatch analysis including CRC verification.
/// </summary>
public class AnalyzeCommand : CommandBase
{
    private readonly Argument<string> _pathArg = new("path")
    {
        Description = "Path to the log segment file to analyze (e.g., data/topic/partition-0/00000000000000000000.log)"
    };

    public AnalyzeCommand() : base("analyze", "Analyze log segment files for RecordBatch structure and CRC integrity")
    {
        Arguments.Add(_pathArg);
        this.SetAction(ExecuteAsync);
    }

    private Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var path = parseResult.GetValue(_pathArg);
        var format = GetFormat(parseResult);

        if (string.IsNullOrEmpty(path))
        {
            WriteError("Log file path is required");
            return Task.FromResult(1);
        }

        // Resolve to absolute path if relative
        var fullPath = Path.GetFullPath(path);

        if (!File.Exists(fullPath))
        {
            WriteError($"File not found: {fullPath}");
            return Task.FromResult(1);
        }

        if (format != OutputFormat.Json && format != OutputFormat.Plain)
        {
            AnsiConsole.Write(new Rule("[bold blue]Log Segment Analysis[/]").LeftJustified());
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine($"[dim]File: {Markup.Escape(fullPath)}[/]");
            AnsiConsole.WriteLine();
        }

        try
        {
            // BatchAnalyzer outputs directly to Console, which works for all output formats
            BatchAnalyzer.AnalyzeLogFile(fullPath);
            return Task.FromResult(0);
        }
        catch (Exception ex)
        {
            WriteError($"Failed to analyze log file: {ex.Message}");
            return Task.FromResult(1);
        }
    }
}
