#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Local dev-loop helper: builds the four premium Control UI plugins
    (Functions / Replication / Governance / AI) as .swpkg and installs
    them idempotently via `surgewave plugins install` into
    src/Kuestenlogik.Surgewave.Control/plugins/.

.DESCRIPTION
    Expects the premium repos as sibling clones next to Surgewave/:
      ..\Surgewave.Functions
      ..\Surgewave.Replication
      ..\Surgewave.Governance
      ..\Surgewave.Ai

    Per plugin:
      1. dotnet publish src/<Plugin>.Control -c Release -p:SurgewavePackPlugin=true
         -> artifacts/pluginPackage/<plugin-id>-<version>.swpkg
      2. surgewave plugins install <swpkg> -d <target-plugins-dir> -f
         (-f implements idempotency: completely overwrites any existing
         old installation; SHA256 is verified)

    Same pipeline as the production install path: the CLI uses
    `PluginPackageManager.InstallAsync`, which the broker and the
    marketplace also use for remote installs.

.PARAMETER SkipBuild
    Skip dotnet publish and only install the .swpkg files that already exist.

.PARAMETER NoClean
    Do NOT empty the whole plugins/ folder first. Default: everything goes,
    clean start; with -NoClean individual re-installs can be tested
    without losing the other plugins.

.EXAMPLE
    pwsh scripts/deploy-control-plugins.ps1
    pwsh scripts/deploy-control-plugins.ps1 -SkipBuild
    pwsh scripts/deploy-control-plugins.ps1 -NoClean
#>

[CmdletBinding()]
param(
    [switch]$SkipBuild,
    [switch]$NoClean
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$PluginsRoot = Join-Path $RepoRoot "src\Kuestenlogik.Surgewave.Control\plugins"
$SurgewaveCli = Join-Path $RepoRoot "artifacts\bin\Kuestenlogik.Surgewave.Tool\Release\surgewave.exe"

if (-not (Test-Path $SurgewaveCli)) {
    Write-Host "  surgewave CLI not built: $SurgewaveCli" -ForegroundColor Red
    Write-Host "  → dotnet build src/Kuestenlogik.Surgewave.Tool -c Release" -ForegroundColor Gray
    exit 1
}

$Plugins = @(
    @{
        Name    = "Functions"
        Repo    = "..\Surgewave.Functions"
        Project = "src\Kuestenlogik.Surgewave.Functions.Control\Kuestenlogik.Surgewave.Functions.Control.csproj"
        Swpkg   = "artifacts\pluginPackage\Kuestenlogik.Surgewave.Functions.Control-0.1.0.swpkg"
    },
    @{
        Name    = "Replication"
        Repo    = "..\Surgewave.Replication"
        Project = "src\Kuestenlogik.Surgewave.Replication.Control\Kuestenlogik.Surgewave.Replication.Control.csproj"
        Swpkg   = "artifacts\pluginPackage\Kuestenlogik.Surgewave.Replication.Control-0.1.0.swpkg"
    },
    @{
        Name    = "Governance"
        Repo    = "..\Surgewave.Governance"
        Project = "src\Kuestenlogik.Surgewave.Governance.Control\Kuestenlogik.Surgewave.Governance.Control.csproj"
        Swpkg   = "artifacts\pluginPackage\Kuestenlogik.Surgewave.Governance.Control-0.1.0.swpkg"
    },
    @{
        Name    = "AI"
        Repo    = "..\Surgewave.Ai"
        Project = "src\Kuestenlogik.Surgewave.AI.Control\Kuestenlogik.Surgewave.AI.Control.csproj"
        Swpkg   = "artifacts\pluginPackage\Kuestenlogik.Surgewave.AI.Control-0.1.0.swpkg"
    }
)

Write-Host ""
Write-Host "  Surgewave Control — Plugin Deploy" -ForegroundColor Cyan
Write-Host "  Target: $PluginsRoot" -ForegroundColor Gray
Write-Host "  CLI:    $SurgewaveCli" -ForegroundColor Gray
Write-Host ""

# Clean start: clear out the whole plugins/ folder, otherwise renamed
# or deleted plugins can survive as stale folders.
# The caller can skip this with -NoClean.
if (-not $NoClean -and (Test-Path $PluginsRoot)) {
    Write-Host "  Cleaning $PluginsRoot..." -ForegroundColor Yellow
    Remove-Item -Path $PluginsRoot -Recurse -Force
}

foreach ($p in $Plugins) {
    $repoPath = Join-Path $RepoRoot $p.Repo
    $repoPath = (Resolve-Path -Path $repoPath -ErrorAction SilentlyContinue)?.Path
    if (-not $repoPath -or -not (Test-Path $repoPath)) {
        Write-Host "  [SKIP] $($p.Name) — repo not found: $($p.Repo)" -ForegroundColor Yellow
        continue
    }

    $projectPath = Join-Path $repoPath $p.Project
    $swpkgPath = Join-Path $repoPath $p.Swpkg

    Write-Host "  → Surgewave.$($p.Name)" -ForegroundColor White

    if (-not $SkipBuild) {
        Write-Host "      Pack...     " -NoNewline
        $publishLog = dotnet publish $projectPath -c Release -p:SurgewavePackPlugin=true --nologo -v quiet 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "FAILED" -ForegroundColor Red
            $publishLog | Select-Object -Last 20 | ForEach-Object { Write-Host "        $_" -ForegroundColor DarkGray }
            exit 1
        }
        Write-Host "OK" -ForegroundColor Green
    }

    if (-not (Test-Path $swpkgPath)) {
        Write-Host "      [ERROR] .swpkg missing: $swpkgPath" -ForegroundColor Red
        exit 1
    }

    # `surgewave plugins install` is the same code path as a
    # marketplace or remote install: SHA256 verify + PluginPackageManager.
    # `-f` overwrites an existing installation of the same name, which
    # makes this local dev loop idempotent.
    Write-Host "      Install...  " -NoNewline
    $installLog = & $SurgewaveCli plugins install $swpkgPath -d $PluginsRoot -f 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "FAILED" -ForegroundColor Red
        $installLog | ForEach-Object { Write-Host "        $_" -ForegroundColor DarkGray }
        exit 1
    }
    Write-Host "OK" -ForegroundColor Green
    # Pass through the "Installed ... v..." line from the CLI, without
    # the "Installing plugin..." spinner texts.
    $installLog |
        Where-Object { $_ -match 'Installed|Upgraded|SHA256' } |
        ForEach-Object { Write-Host "        $_" -ForegroundColor Gray }
}

Write-Host ""
Write-Host "  Done. Start Control:" -ForegroundColor Green
Write-Host "  dotnet run --project src/Kuestenlogik.Surgewave.Control --urls http://localhost:5050" -ForegroundColor Gray
Write-Host ""
