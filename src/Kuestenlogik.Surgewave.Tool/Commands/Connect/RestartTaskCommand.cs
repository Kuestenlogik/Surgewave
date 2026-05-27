using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;

namespace Kuestenlogik.Surgewave.Cli.Commands.Connect;

/// <summary>
/// Restart a specific task (surgewave connect tasks restart)
/// </summary>
public class RestartTaskCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("connector") { Description = "Connector name" };
    private readonly Argument<int> _taskIdArg = new("task-id") { Description = "Task ID" };

    public RestartTaskCommand() : base("restart", "Restart a specific task")
    {
        Arguments.Add(_nameArg);
        Arguments.Add(_taskIdArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var name = parseResult.GetValue(_nameArg);
        var taskId = parseResult.GetValue(_taskIdArg);

        WriteVerbose(parseResult, $"Restarting task {taskId} for connector '{name}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            await client.Connect.RestartConnectorTaskAsync(name, taskId, ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new { Connector = name, TaskId = taskId, Restarted = true }, ConnectJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"restarted {name}/{taskId}");
            }
            else
            {
                WriteSuccess($"Restarted task {taskId} for connector '{name}'");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to restart task: {ex.Message}");
            return 1;
        }
    }
}
