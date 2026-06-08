using System.CommandLine;
using System.CommandLine.Parsing;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Sdk;

/// <summary>
/// <c>surgewave sdk install --version X.Y.Z</c> — pull the .nupkg assets
/// of a tagged Surgewave GitHub Release into a local feed so plugin
/// projects can resolve <c>Kuestenlogik.Surgewave.Sdk</c> at a specific
/// version without depending on the corporate NuGet feed or hardcoding
/// a relative path. Implements G — Plugin SDK D.
/// </summary>
public sealed class InstallSdkCommand : CommandBase
{
    private readonly Option<string> _versionOpt = new("--version", "-v")
    {
        Description = "SDK version to install (e.g. 0.1.13, v0.1.13, or 'latest').",
        DefaultValueFactory = _ => "latest",
    };

    private readonly Option<string> _ownerOpt = new("--owner")
    {
        Description = "GitHub owner / org hosting the Surgewave releases.",
        DefaultValueFactory = _ => "Kuestenlogik",
    };

    private readonly Option<string> _repoOpt = new("--repo")
    {
        Description = "Repository name on the GitHub owner.",
        DefaultValueFactory = _ => "Surgewave",
    };

    private readonly Option<string?> _installDirOpt = new("--install-dir")
    {
        Description = "Local feed directory. Defaults to '~/.surgewave/sdk/<version>'.",
    };

    private readonly Option<bool> _forceOpt = new("--force", "-f")
    {
        Description = "Re-download already-present .nupkg files.",
    };

    private readonly Option<string?> _writeConfigOpt = new("--write-nuget-config")
    {
        Description = "Path to a plugin project directory. A nuget.config is written there pointing at the local feed.",
    };

    public InstallSdkCommand() : base("install", "Download the Surgewave SDK .nupkg assets into a local feed")
    {
        Options.Add(_versionOpt);
        Options.Add(_ownerOpt);
        Options.Add(_repoOpt);
        Options.Add(_installDirOpt);
        Options.Add(_forceOpt);
        Options.Add(_writeConfigOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var version = parseResult.GetValue(_versionOpt) ?? "latest";
        var owner = parseResult.GetValue(_ownerOpt) ?? "Kuestenlogik";
        var repo = parseResult.GetValue(_repoOpt) ?? "Surgewave";
        var installDir = parseResult.GetValue(_installDirOpt);
        var force = parseResult.GetValue(_forceOpt);
        var writeConfig = parseResult.GetValue(_writeConfigOpt);

        using var http = new HttpClient();
        var installer = new SdkInstaller(http, msg => WriteMarkup($"[dim]  {msg}[/]"));

        string tag;
        try
        {
            tag = await AnsiConsole.Status().StartAsync(
                $"Resolving {owner}/{repo}@{version}...",
                async _ => await installer.ResolveTagAsync(owner, repo, version, ct));
        }
        catch (Exception ex)
        {
            WriteError($"Failed to resolve version '{version}' on {owner}/{repo}: {ex.Message}");
            return 1;
        }

        var resolvedVersion = tag.TrimStart('v');
        installDir ??= Path.Combine(SdkInstaller.DefaultSdkRoot, resolvedVersion);
        WriteMarkup($"[bold]Resolved:[/] {tag}  [dim]→[/] {installDir}");

        IReadOnlyList<NupkgAsset> assets;
        try
        {
            assets = await AnsiConsole.Status().StartAsync(
                $"Listing .nupkg assets on {tag}...",
                async _ => await installer.ListNupkgAssetsAsync(owner, repo, tag, ct));
        }
        catch (Exception ex)
        {
            WriteError($"Failed to list assets on {tag}: {ex.Message}");
            return 1;
        }

        WriteMarkup($"[bold]Assets:[/] {assets.Count} .nupkg ({assets.Sum(a => a.SizeBytes) / (1024 * 1024)} MB total)");

        DownloadResult result;
        try
        {
            result = await installer.DownloadAsync(assets, installDir, force, ct);
        }
        catch (Exception ex)
        {
            WriteError($"Download failed: {ex.Message}");
            return 1;
        }

        WriteSuccess($"SDK {resolvedVersion} ready at {result.TargetDirectory} ({result.Downloaded} new, {result.Skipped} skipped)");

        if (!string.IsNullOrWhiteSpace(writeConfig))
        {
            try
            {
                SdkInstaller.WriteNugetConfig(writeConfig, installDir);
                WriteSuccess($"Wrote nuget.config in {writeConfig} pointing at the local feed");
            }
            catch (Exception ex)
            {
                WriteError($"nuget.config write failed: {ex.Message}");
                return 1;
            }
        }
        else
        {
            WriteMarkup("[dim]Tip: pass --write-nuget-config <project-dir> to wire a plugin project at the same time, or add manually:[/]");
            WriteMarkup($"[dim]  <add key=\"surgewave-sdk-local\" value=\"{installDir}\" /> in <packageSources>[/]");
        }

        return 0;
    }
}
