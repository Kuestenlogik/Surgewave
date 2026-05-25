using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;

namespace Kuestenlogik.Surgewave.Cli.Commands.Connect;

/// <summary>
/// Create a connector (surgewave connect create)
/// </summary>
public class CreateConnectorCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("name") { Description = "Connector name" };
    private readonly Option<string?> _configOpt = new("--config", "-c") { Description = "Configuration as JSON string" };
    private readonly Option<string?> _fileOpt = new("--file", "-f") { Description = "Read configuration from JSON file" };

    public CreateConnectorCommand() : base("create", "Create a new connector")
    {
        Arguments.Add(_nameArg);
        Options.Add(_configOpt);
        Options.Add(_fileOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var name = parseResult.GetValue(_nameArg);
        var configJson = parseResult.GetValue(_configOpt);
        var configFile = parseResult.GetValue(_fileOpt);

        // Read config from file if specified
        if (!string.IsNullOrEmpty(configFile))
        {
            if (!File.Exists(configFile))
            {
                WriteError($"Config file not found: {configFile}");
                return 1;
            }
            configJson = await File.ReadAllTextAsync(configFile, ct);
        }

        if (string.IsNullOrEmpty(configJson))
        {
            WriteError("Configuration is required. Use --config or --file");
            return 1;
        }

        // Validate JSON
        Dictionary<string, string> config;
        try
        {
            config = JsonSerializer.Deserialize<Dictionary<string, string>>(configJson) ?? new();
        }
        catch (JsonException)
        {
            WriteError("Invalid JSON configuration");
            return 1;
        }

        if (!config.ContainsKey("connector.class"))
        {
            WriteError("Configuration must include 'connector.class'");
            return 1;
        }

        WriteVerbose(parseResult, $"Creating connector '{name}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var result = await client.Connect.CreateConnectorAsync(name, config, ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new
                {
                    Name = result.Name,
                    TaskCount = result.TaskCount
                }, ConnectJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"created {result.Name} tasks={result.TaskCount}");
            }
            else
            {
                WriteSuccess($"Created connector '{result.Name}' with {result.TaskCount} task(s)");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to create connector: {ex.Message}");
            return 1;
        }
    }
}
