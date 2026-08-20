using System.CommandLine;
using System.CommandLine.Parsing;
using System.Diagnostics;
using Kuestenlogik.Surgewave.Pipelines;
using Kuestenlogik.Surgewave.Pipelines.Publishing;
using Kuestenlogik.Surgewave.Pipelines.Serialization;

namespace Kuestenlogik.Surgewave.Cli.Commands.Pipelines;

/// <summary>
/// Deploy pipelines to the broker (surgewave pipelines deploy).
/// Accepts a pipeline export JSON, a compiled pipeline-as-code assembly, or a project —
/// projects are built with the dotnet SDK first. With --watch, source changes trigger
/// rebuild and redeploy (hot reload on save).
/// </summary>
public class DeployPipelineCommand : CommandBase
{
    private readonly Argument<string> _targetArg = new("target")
    {
        Description = "Pipeline export (.json), compiled pipeline library (.dll), project (.csproj), or project directory"
    };

    private readonly Option<string?> _nameOpt = new("--name")
    {
        Description = "Deploy under this pipeline name (single-pipeline targets only)"
    };

    private readonly Option<bool> _replaceOpt = new("--replace")
    {
        Description = "Replace an existing pipeline with the same name (stops and restarts it when running)"
    };

    private readonly Option<bool> _startOpt = new("--start")
    {
        Description = "Start the pipeline after deploying"
    };

    private readonly Option<bool> _watchOpt = new("--watch", "-w")
    {
        Description = "Watch the source for changes and redeploy on save (implies --replace)"
    };

    public DeployPipelineCommand() : base("deploy", "Deploy pipelines from a JSON export, a compiled library, or a project")
    {
        Arguments.Add(_targetArg);
        Options.Add(_nameOpt);
        Options.Add(_replaceOpt);
        Options.Add(_startOpt);
        Options.Add(_watchOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var target = parseResult.GetValue(_targetArg)!;
        var nameOverride = parseResult.GetValue(_nameOpt);
        var watch = parseResult.GetValue(_watchOpt);
        var replace = parseResult.GetValue(_replaceOpt) || watch;
        var start = parseResult.GetValue(_startOpt);

        var options = new PipelinePublishOptions
        {
            Mode = replace ? PublishMode.ReplaceByName : PublishMode.CreateNew,
            NameOverride = nameOverride,
            Start = start,
        };

        try
        {
            using var publisher = PipelineCli.CreatePublisher(parseResult);

            var exitCode = await DeployOnceAsync(publisher, target, options, parseResult, ct);
            if (!watch || exitCode != 0)
            {
                return exitCode;
            }

            return await WatchAsync(publisher, target, options, parseResult, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return 0;
        }
        catch (Exception ex) when (ex is PipelinePublishException or PipelineBuildException or IOException or System.Text.Json.JsonException)
        {
            WriteError(ex.Message);
            return 1;
        }
    }

    private async Task<int> DeployOnceAsync(
        PipelinePublisher publisher,
        string target,
        PipelinePublishOptions options,
        ParseResult parseResult,
        CancellationToken ct)
    {
        var pipelines = await CollectPipelinesAsync(target, parseResult, ct);
        if (pipelines.Count == 0)
        {
            WriteError($"No pipelines found in '{target}'. A pipeline library exposes classes implementing ISurgewavePipeline.");
            return 1;
        }

        if (options.NameOverride is not null && pipelines.Count > 1)
        {
            WriteError($"--name cannot be applied: '{target}' contains {pipelines.Count} pipelines.");
            return 1;
        }

        foreach (var scanned in pipelines)
        {
            var result = await publisher.PublishAsync(scanned.Pipeline, options, ct);
            var action = result.Replaced ? "updated" : "created";
            var state = result.Started ? ", started" : "";
            WriteSuccess($"Pipeline '{result.Name}' {action} ({result.PipelineId}{state}).");
        }

        return 0;
    }

    private async Task<IReadOnlyList<ScannedPipeline>> CollectPipelinesAsync(
        string target,
        ParseResult parseResult,
        CancellationToken ct)
    {
        if (target.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
        {
            var export = PipelineJson.ReadFile(target);
            var scanned = new ScannedPipeline
            {
                TypeName = Path.GetFileName(target),
                Pipeline = ExportToBuilt(export),
            };
            return [scanned];
        }

        var assemblyPath = target.EndsWith(".dll", StringComparison.OrdinalIgnoreCase)
            ? target
            : await BuildProjectAsync(target, parseResult, ct);

        return PipelineAssemblyScanner.Scan(assemblyPath);
    }

    private static BuiltPipeline ExportToBuilt(Kuestenlogik.Surgewave.Connect.Pipelines.PipelineExportFormat export)
    {
        var data = export.Pipeline;
        return new BuiltPipeline
        {
            Name = string.IsNullOrWhiteSpace(data.Name) ? null : data.Name,
            Description = data.Description,
            Nodes = data.Nodes.Select(n => new Kuestenlogik.Surgewave.Connect.Pipelines.PipelineNode
            {
                Id = n.NodeId,
                ConnectorType = n.ConnectorType,
                Config = new Dictionary<string, string>(n.Config),
                X = n.X,
                Y = n.Y,
                Label = n.Label,
                RetryPolicy = n.RetryPolicy,
            }).ToList(),
            Connections = data.Connections.Select((c, index) => new Kuestenlogik.Surgewave.Connect.Pipelines.PipelineConnection
            {
                Id = $"c{index + 1}",
                SourceNodeId = c.SourceNodeId,
                TargetNodeId = c.TargetNodeId,
                Type = c.Type,
            }).ToList(),
            Parameters = data.Parameters,
            Schedule = data.Schedule,
        };
    }

    private async Task<string> BuildProjectAsync(string target, ParseResult parseResult, CancellationToken ct)
    {
        var projectPath = ResolveProjectPath(target);
        WriteVerbose(parseResult, $"Building {projectPath}...");

        var build = await RunDotnetAsync(["build", projectPath, "--nologo", "-v", "q"], ct);
        if (build.ExitCode != 0)
        {
            throw new PipelineBuildException($"dotnet build failed for {projectPath}:\n{build.Stdout}\n{build.Stderr}".Trim());
        }

        var probe = await RunDotnetAsync(["msbuild", projectPath, "-getProperty:TargetPath", "--nologo"], ct);
        // parse stdout only — SDK warnings on stderr must not corrupt the path
        var targetPath = probe.Stdout.Trim();
        if (probe.ExitCode != 0 || targetPath.Length == 0 || !File.Exists(targetPath))
        {
            throw new PipelineBuildException(
                $"Could not determine the build output of {projectPath}." +
                (targetPath.Length == 0
                    ? " Multi-targeted projects have no single TargetPath — pass the built .dll directly."
                    : $" ({targetPath})"));
        }

        return targetPath;
    }

    private static string ResolveProjectPath(string target)
    {
        if (target.EndsWith(".csproj", StringComparison.OrdinalIgnoreCase))
        {
            return Path.GetFullPath(target);
        }

        if (Directory.Exists(target))
        {
            var projects = Directory.GetFiles(target, "*.csproj");
            return projects.Length switch
            {
                1 => Path.GetFullPath(projects[0]),
                0 => throw new PipelineBuildException($"No .csproj found in '{target}'."),
                _ => throw new PipelineBuildException($"'{target}' contains {projects.Length} projects — pass the .csproj explicitly."),
            };
        }

        throw new FileNotFoundException($"Deploy target not found: {target}");
    }

    private static async Task<(int ExitCode, string Stdout, string Stderr)> RunDotnetAsync(string[] arguments, CancellationToken ct)
    {
        var startInfo = new ProcessStartInfo("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in arguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        using var process = Process.Start(startInfo)
            ?? throw new PipelineBuildException("Could not start 'dotnet' — is the .NET SDK installed?");

        var stdout = process.StandardOutput.ReadToEndAsync(ct);
        var stderr = process.StandardError.ReadToEndAsync(ct);
        await process.WaitForExitAsync(ct);
        return (process.ExitCode, await stdout, await stderr);
    }

    private async Task<int> WatchAsync(
        PipelinePublisher publisher,
        string target,
        PipelinePublishOptions options,
        ParseResult parseResult,
        CancellationToken ct)
    {
        var watchDir = ResolveWatchDirectory(target);

        // When watching a project we build ourselves, our own build output must not
        // retrigger the loop. A .dll or .json target IS the build output — filtering
        // bin/obj there would eat every event.
        var filterBuildOutput = !target.EndsWith(".dll", StringComparison.OrdinalIgnoreCase)
            && !target.EndsWith(".json", StringComparison.OrdinalIgnoreCase);

        WriteLine($"Watching {watchDir} — deploying on save (Ctrl+C to stop).");

        using var trigger = new SemaphoreSlim(0);
        using var watcher = new FileSystemWatcher(watchDir)
        {
            IncludeSubdirectories = true,
            NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.FileName,
        };

        foreach (var filter in (string[])["*.cs", "*.csproj", "*.json", "*.dll"])
        {
            watcher.Filters.Add(filter);
        }

        watcher.Changed += OnChange;
        watcher.Created += OnChange;
        watcher.Renamed += OnChange;
        watcher.EnableRaisingEvents = true;

        while (!ct.IsCancellationRequested)
        {
            await trigger.WaitAsync(ct);

            // debounce: editors and builds fire bursts of events
            await Task.Delay(TimeSpan.FromMilliseconds(400), ct);
            while (trigger.CurrentCount > 0)
            {
                await trigger.WaitAsync(ct);
            }

            WriteLine($"[{DateTime.Now:HH:mm:ss}] Change detected — redeploying...");
            try
            {
                await DeployOnceAsync(publisher, target, options, parseResult, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // stay in the watch loop whatever went wrong (locked files mid-save,
                // half-written dlls, broker restarts) — the next save gets another chance
                WriteError(ex.Message);
            }
        }

        return 0;

        void OnChange(object sender, FileSystemEventArgs e)
        {
            if (filterBuildOutput)
            {
                var path = e.FullPath.Replace('\\', '/');
                if (path.Contains("/obj/", StringComparison.OrdinalIgnoreCase) || path.Contains("/bin/", StringComparison.OrdinalIgnoreCase))
                {
                    return;
                }
            }

            trigger.Release();
        }
    }

    private static string ResolveWatchDirectory(string target)
    {
        if (Directory.Exists(target))
        {
            return Path.GetFullPath(target);
        }

        return Path.GetDirectoryName(Path.GetFullPath(target)) is { Length: > 0 } dir
            ? dir
            : Directory.GetCurrentDirectory();
    }
}
