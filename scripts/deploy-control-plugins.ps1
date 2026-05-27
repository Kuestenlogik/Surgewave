#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Local dev-loop helper: baut die vier Control-UI-Premium-Plugins
    (Functions / Replication / Governance / AI) als .swpkg und installiert
    sie idempotent ueber `surgewave plugins install` in
    src/Kuestenlogik.Surgewave.Control/plugins/.

.DESCRIPTION
    Erwartet die Premium-Repos als Sibling-Klone neben Surgewave/:
      ..\Surgewave.Functions
      ..\Surgewave.Replication
      ..\Surgewave.Governance
      ..\Surgewave.Ai

    Pro Plugin:
      1. dotnet publish src/<Plugin>.Control -c Release -p:SurgewavePackPlugin=true
         -> artifacts/pluginPackage/<plugin-id>-<version>.swpkg
      2. surgewave plugins install <swpkg> -d <target-plugins-dir> -f
         (-f setzt Idempotenz um: ueberschreibt eine evtl. vorhandene
         alte Installation komplett; SHA256 wird verifiziert)

    Identische Pipeline wie der Production-Install-Path: das CLI nutzt
    `PluginPackageManager.InstallAsync`, das auch der Broker und das
    Marketplace fuer Remote-Installs verwenden.

.PARAMETER SkipBuild
    Skip dotnet publish und nur die bereits vorhandenen .swpkg installieren.

.PARAMETER NoClean
    Komplette plugins/-Folder NICHT vorher leeren. Default: alles weg,
    sauberer Start; mit -NoClean koennen einzelne Re-Installs getestet
    werden ohne die anderen Plugins zu verlieren.

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
    Write-Host "  surgewave CLI nicht gebaut: $SurgewaveCli" -ForegroundColor Red
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

# Sauberer Start: das ganze plugins/ wegraeumen, sonst koennen
# umbenannte oder geloeschte Plugins als stale Folder ueberleben.
# Mit -NoClean kann der Caller das ueberspringen.
if (-not $NoClean -and (Test-Path $PluginsRoot)) {
    Write-Host "  Cleaning $PluginsRoot..." -ForegroundColor Yellow
    Remove-Item -Path $PluginsRoot -Recurse -Force
}

foreach ($p in $Plugins) {
    $repoPath = Join-Path $RepoRoot $p.Repo
    $repoPath = (Resolve-Path -Path $repoPath -ErrorAction SilentlyContinue)?.Path
    if (-not $repoPath -or -not (Test-Path $repoPath)) {
        Write-Host "  [SKIP] $($p.Name) — Repo nicht gefunden: $($p.Repo)" -ForegroundColor Yellow
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
        Write-Host "      [ERROR] .swpkg fehlt: $swpkgPath" -ForegroundColor Red
        exit 1
    }

    # `surgewave plugins install` ist derselbe Code-Path wie ein
    # Marketplace- oder Remote-Install: SHA256-Verify + PluginPackageManager.
    # `-f` ueberschreibt eine vorhandene gleichnamige Installation, was
    # diesen lokalen Dev-Loop idempotent macht.
    Write-Host "      Install...  " -NoNewline
    $installLog = & $SurgewaveCli plugins install $swpkgPath -d $PluginsRoot -f 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "FAILED" -ForegroundColor Red
        $installLog | ForEach-Object { Write-Host "        $_" -ForegroundColor DarkGray }
        exit 1
    }
    Write-Host "OK" -ForegroundColor Green
    # Die "Installed ... v..." -Zeile aus dem CLI durchreichen, ohne
    # die "Installing plugin..." Spinner-Texte.
    $installLog |
        Where-Object { $_ -match 'Installed|Upgraded|SHA256' } |
        ForEach-Object { Write-Host "        $_" -ForegroundColor Gray }
}

Write-Host ""
Write-Host "  Done. Start Control:" -ForegroundColor Green
Write-Host "  dotnet run --project src/Kuestenlogik.Surgewave.Control --urls http://localhost:5050" -ForegroundColor Gray
Write-Host ""
