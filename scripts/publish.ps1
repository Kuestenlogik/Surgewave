#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Publishes self-contained Surgewave executables and/or container images.
.DESCRIPTION
    Runs `dotnet publish` for each Surgewave service (Broker, Gateway, Control,
    Marketplace, Connector, Cli) and writes the result to
    artifacts/pub/<Name>/<Runtime>/.
    Optionally builds local container images via .NET's native container
    support (no Dockerfile needed). The CLI is excluded from container builds
    since it is a command-line tool rather than a service.
    `dotnet publish` performs its own implicit build, so this script is
    independent of `build.ps1`. Use `build.ps1` only when you need fresh
    NuGet packages (e.g. for the Samples solution).
.PARAMETER Runtime
    Target runtime for the published executables (default: win-x64).
.PARAMETER Configuration
    Build configuration (default: Release).
.PARAMETER Service
    Optional list of service names to publish. Default: all services.
    Valid values: Broker, Gateway, Control, Marketplace, Connector, Cli.
.PARAMETER Mode
    What to publish: Executable, Container, or All (default: All).
.EXAMPLE
    .\scripts\publish.ps1
.EXAMPLE
    .\scripts\publish.ps1 -Runtime linux-x64
.EXAMPLE
    .\scripts\publish.ps1 -Mode Container
.EXAMPLE
    .\scripts\publish.ps1 -Service Broker,Control
#>
[CmdletBinding()]
param(
    [string]$Runtime = "win-x64",
    [string]$Configuration = "Release",
    [string[]]$Service,
    [ValidateSet("Executable", "Container", "All")]
    [string]$Mode = "All"
)
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$publishDir = Join-Path $root "artifacts" "pub"
$allServices = @(
    @{ Name = "Broker";      Project = "src/Kuestenlogik.Surgewave.Broker";          Container = $true  }
    @{ Name = "Gateway";     Project = "src/Kuestenlogik.Surgewave.Gateway";         Container = $true  }
    @{ Name = "Control";     Project = "src/Kuestenlogik.Surgewave.Control";         Container = $true  }
    @{ Name = "Marketplace"; Project = "src/Kuestenlogik.Surgewave.Marketplace";     Container = $true  }
    @{ Name = "Connector";   Project = "src/Kuestenlogik.Surgewave.Connect.Worker";  Container = $true  }
    @{ Name = "Cli";         Project = "src/Kuestenlogik.Surgewave.Tool";            Container = $true  }
)
# Filter to requested services if -Service was provided
$services = if ($Service -and $Service.Count -gt 0) {
    $allServices | Where-Object { $Service -contains $_.Name }
} else {
    $allServices
}
if ($services.Count -eq 0) {
    Write-Host ""
    Write-Host "  ERROR: no matching services. Valid names: $(($allServices | ForEach-Object { $_.Name }) -join ', ')" -ForegroundColor Red
    Write-Host ""
    exit 1
}
$doExecutable = $Mode -eq "Executable" -or $Mode -eq "All"
$doContainer  = $Mode -eq "Container"  -or $Mode -eq "All"
Write-Host ""
Write-Host "  Surgewave — Publish" -ForegroundColor Cyan
Write-Host "  Mode:          $Mode" -ForegroundColor Gray
Write-Host "  Runtime:       $Runtime" -ForegroundColor Gray
Write-Host "  Configuration: $Configuration" -ForegroundColor Gray
Write-Host "  Services:      $(($services | ForEach-Object { $_.Name }) -join ', ')" -ForegroundColor Gray
Write-Host ""
$failed = 0
$sw = [System.Diagnostics.Stopwatch]::StartNew()
# ── Self-contained executables ───────────────────────────────────────────────
if ($doExecutable) {
    Write-Host "━━━ Optimized Executables ($Runtime) ━━━" -ForegroundColor Yellow
    foreach ($svc in $services) {
        $project = Join-Path $root $svc.Project
        $outDir = Join-Path $publishDir "apps" $svc.Name.ToLower() $Runtime
        if (-not (Test-Path $project)) {
            Write-Host "  $($svc.Name)... project missing, skipped" -ForegroundColor DarkGray
            continue
        }
        Write-Host "  $($svc.Name)..." -NoNewline
        dotnet publish $project -c $Configuration -r $Runtime -p:Optimized=true -o $outDir --nologo -v quiet 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Host " FAILED" -ForegroundColor Red
            $failed++
        } else {
            $size = [math]::Round((Get-ChildItem -Path $outDir -Recurse -File | Measure-Object -Property Length -Sum).Sum / 1MB, 1)
            Write-Host " ${size} MB → $outDir" -ForegroundColor Green
        }
        # After Broker: pack and install community protocol plugins via MSBuild targets.
        # SurgewavePackPlugin + SurgewaveInstallPlugin targets run as part of dotnet publish.
        # Logic is in Kuestenlogik.Surgewave.Build (PackPluginTask / InstallPluginTask) which delegates
        # to PluginPackageManager — the same code used by `surgewave plugins pack/install`.
        if ($svc.Name -eq "Broker" -and $LASTEXITCODE -eq 0) {
            $communityProtocols = @(
                @{ Name = "Kafka";      Project = "src/Kuestenlogik.Surgewave.Protocol.Kafka";      Id = "kuestenlogik.surgewave.protocol.kafka"      }
                @{ Name = "Mqtt";       Project = "src/Kuestenlogik.Surgewave.Protocol.Mqtt";       Id = "kuestenlogik.surgewave.protocol.mqtt"       }
                @{ Name = "WebSocket";  Project = "src/Kuestenlogik.Surgewave.Protocol.WebSocket";  Id = "kuestenlogik.surgewave.protocol.websocket"  }
                @{ Name = "Amqp";       Project = "src/Kuestenlogik.Surgewave.Protocol.Amqp";       Id = "kuestenlogik.surgewave.protocol.amqp"       }
                @{ Name = "PostgreSql"; Project = "src/Kuestenlogik.Surgewave.Protocol.PostgreSql"; Id = "kuestenlogik.surgewave.protocol.postgresql" }
            )
            $pluginsDir       = Join-Path $outDir "plugins"
            # pub\packages\ is the permanent home for community protocol .swpkg packages — same
            # location as a manual `dotnet publish -p:SurgewavePackPlugin=true` would produce.
            # These are the distributable artifacts; the intermediate publish output is temp.
            $pluginsPublishDir = Join-Path $publishDir "packages"
            # Start with a clean broker plugins/ dir so stale installs don't accumulate.
            if (Test-Path $pluginsDir) { Remove-Item $pluginsDir -Recurse -Force }
            New-Item -ItemType Directory -Force -Path $pluginsDir | Out-Null
            New-Item -ItemType Directory -Force -Path $pluginsPublishDir | Out-Null
            foreach ($proto in $communityProtocols) {
                $protoProject = Join-Path $root $proto.Project
                if (-not (Test-Path $protoProject)) {
                    Write-Host "    plugin: $($proto.Name)... project missing, skipped" -ForegroundColor DarkGray
                    continue
                }
                Write-Host "    plugin: $($proto.Name)..." -NoNewline
                # Publish to a per-protocol temp dir (-o) so the intermediate framework-dependent
                # output doesn't land in the permanent artifacts/pub tree. The MSBuild tasks
                # read $(PublishDir) (= $protoStageDir), pack a .swpkg into $pluginsPublishDir,
                # and install it into the broker's plugins/ dir. The staging dir is deleted after.
                $protoStageDir = Join-Path $root "artifacts" "tmp" (Split-Path -Leaf $proto.Project)
                dotnet publish $protoProject -c $Configuration `
                    -o $protoStageDir `
                    -p:SurgewavePackPlugin=true `
                    -p:SurgewaveInstallPlugin=true `
                    "-p:SurgewaveSppOutputDir=$pluginsPublishDir" `
                    "-p:SurgewavePluginsDir=$pluginsDir" `
                    --nologo -v quiet 2>&1 | Out-Null
                # MSBuild's SurgewaveCleanupPublish target already removes the staging dir;
                # this is a safety net in case the build failed before cleanup ran.
                if (Test-Path $protoStageDir) { Remove-Item $protoStageDir -Recurse -Force -ErrorAction SilentlyContinue }
                if ($LASTEXITCODE -ne 0) {
                    Write-Host " FAILED" -ForegroundColor Red
                    $failed++
                } else {
                    $pluginPackageFile = Get-ChildItem $pluginsPublishDir -Filter "$($proto.Id)-*.swpkg" |
                        Sort-Object LastWriteTime -Descending | Select-Object -First 1
                    $sizeMB = if ($pluginPackageFile) { [math]::Round($pluginPackageFile.Length / 1MB, 1) } else { "?" }
                    Write-Host " ${sizeMB} MB → packages/$($pluginPackageFile.Name)" -ForegroundColor Green
                }
            }
        }
    }
    Write-Host ""
}
# ── Container images (local Docker daemon via .NET native containers) ────────
if ($doContainer) {
    Write-Host "━━━ Container Images (local) ━━━" -ForegroundColor Yellow
    $containerServices = $services | Where-Object { $_.Container }
    if ($containerServices.Count -eq 0) {
        Write-Host "  No container-capable services selected." -ForegroundColor DarkGray
    } else {
        # Check if Docker daemon is reachable
        $dockerAvailable = $false
        try {
            docker info 2>&1 | Out-Null
            $dockerAvailable = ($LASTEXITCODE -eq 0)
        } catch { }
        if ($dockerAvailable) {
            Write-Host "  Docker daemon found — tar archives will also be loaded into Docker." -ForegroundColor DarkGray
        } else {
            Write-Host "  Docker daemon not running — only tar archives will be exported (load later with 'docker load -i <file>')." -ForegroundColor DarkGray
        }
        Write-Host ""
        $containerDir = Join-Path $publishDir "containers"
        New-Item -ItemType Directory -Force -Path $containerDir | Out-Null
        foreach ($svc in $containerServices) {
            $project = Join-Path $root $svc.Project
            if (-not (Test-Path $project)) {
                Write-Host "  $($svc.Name)... project missing, skipped" -ForegroundColor DarkGray
                continue
            }
            Write-Host "  $($svc.Name)..." -NoNewline
            $tarPath = Join-Path $containerDir "$($svc.Name.ToLower()).tar"
            # The .tar is the portable artifact — always produced. Staging output goes
            # to artifacts/tmp/ to keep artifacts/pub/ clean.
            $containerStageDir = Join-Path $root "artifacts" "tmp" (Split-Path -Leaf $svc.Project)
            dotnet publish $project -c $Configuration --os linux --arch x64 `
                /t:PublishContainer `
                "-p:ContainerArchiveOutputPath=$tarPath" `
                -o $containerStageDir `
                --nologo -v quiet 2>&1 | Out-Null
            # Clean up the staging directory — only the image/tar matters
            if (Test-Path $containerStageDir) {
                Remove-Item $containerStageDir -Recurse -Force -ErrorAction SilentlyContinue
            }
            if ($LASTEXITCODE -ne 0) {
                Write-Host " FAILED" -ForegroundColor Red
                $failed++
            } else {
                $sizeMB = if (Test-Path $tarPath) {
                    [math]::Round((Get-Item $tarPath).Length / 1MB, 1)
                } else { "?" }
                # If Docker is available, load the tar into the local daemon too.
                if ($dockerAvailable) {
                    docker load -i $tarPath 2>&1 | Out-Null
                    if ($LASTEXITCODE -eq 0) {
                        Write-Host " ${sizeMB} MB → $tarPath (loaded into Docker)" -ForegroundColor Green
                    } else {
                        Write-Host " ${sizeMB} MB → $tarPath (docker load failed)" -ForegroundColor Yellow
                    }
                } else {
                    Write-Host " ${sizeMB} MB → $tarPath" -ForegroundColor Green
                }
            }
        }
    }
    Write-Host ""
}
# ── Final cleanup: remove artifacts/tmp/ if empty ───────────────────────────
$stageRoot = Join-Path $root "artifacts" "tmp"
if ((Test-Path $stageRoot) -and -not (Get-ChildItem $stageRoot -Force)) {
    Remove-Item $stageRoot -Recurse -Force -ErrorAction SilentlyContinue
}
# ── Summary ─────────────────────────────────────────────────────────────────
$sw.Stop()
$elapsed = "{0:mm}:{0:ss}" -f $sw.Elapsed
if ($failed -gt 0) {
    Write-Host "  $failed service(s) failed (elapsed $elapsed)" -ForegroundColor Red
    exit 1
} else {
    Write-Host "  Publish done (elapsed $elapsed)" -ForegroundColor Green
    Write-Host ""
    Write-Host "  Next: .\scripts\start.ps1" -ForegroundColor Gray
}
Write-Host ""
