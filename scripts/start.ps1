#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Starts the published Surgewave services from artifacts/pub/ and adds the
    surgewave CLI to PATH for the current PowerShell session.
.DESCRIPTION
    Launches Broker, Gateway, Control and Marketplace — each in its own
    console window so log output stays readable side-by-side. Assumes
    `publish.ps1` has already been run and that artifacts/pub/apps/{broker,
    gateway,control,marketplace}/<runtime>/ contain the self-contained
    executables.

    The script also tries to add artifacts/pub/apps/cli/<runtime>/ to the
    front of $env:PATH so the `surgewave` CLI is immediately usable. Because
    PowerShell scripts run in a child scope, this only works when the
    script is **dot-sourced**:

        . .\scripts\start.ps1

    When invoked normally (.\scripts\start.ps1) the services still start
    correctly, but the script prints the manual command needed to add the
    CLI to PATH afterwards. The PATH change is session-local — closing the
    pwsh window restores the previous environment.

    Each service is started with its CWD set to its publish directory so
    relative paths (plugins/, appsettings.json, wwwroot/) resolve correctly.
.PARAMETER Runtime
    Target runtime folder (default: win-x64).
.PARAMETER ControlUrl
    Display URL for the Control UI — used for the endpoint summary only.
    The actual listen address is configured in Control's appsettings.json via Kestrel:Endpoints:Http:Url.
.PARAMETER MarketplaceUrl
    Display URL for the Marketplace — used for the endpoint summary only.
    The actual listen address is configured in Marketplace's appsettings.json via Kestrel:Endpoints:Http:Url.
.PARAMETER PostgreSql
    Enable the PostgreSQL wire protocol on the broker (port 5432).
    Default: disabled — use this flag only when port 5432 is free (no local PostgreSQL server running).
.PARAMETER SkipBroker
    Don't start the broker.
.PARAMETER SkipGateway
    Don't start the gateway.
.PARAMETER SkipControl
    Don't start the control UI.
.PARAMETER SkipMarketplace
    Don't start the marketplace.
.PARAMETER SkipCli
    Don't add the surgewave CLI to PATH.
.PARAMETER Wait
    Block this script until the user presses Ctrl+C, then attempt to
    stop the spawned services. Without -Wait the script returns
    immediately and the windows keep running independently.
.EXAMPLE
    . .\scripts\start.ps1
    Dot-source so the surgewave CLI is added to the current session's PATH.
.EXAMPLE
    .\scripts\start.ps1 -PostgreSql -SkipGateway
.EXAMPLE
    .\scripts\start.ps1 -Runtime linux-x64
#>
[CmdletBinding()]
param(
    [string]$Runtime = "win-x64",
    [string]$ControlUrl = "http://localhost:5050",
    [string]$MarketplaceUrl = "http://localhost:8081",
    [switch]$PostgreSql,
    [switch]$SkipBroker,
    [switch]$SkipGateway,
    [switch]$SkipControl,
    [switch]$SkipMarketplace,
    [switch]$SkipCli,
    [switch]$Wait
)
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$publishDir = Join-Path $root "artifacts" "pub"
# ── Service definitions ─────────────────────────────────────────────────────
$appsDir = Join-Path $publishDir "apps"
$services = @(
    @{
        Name      = "Broker"
        Skip      = $SkipBroker
        Dir       = Join-Path $appsDir "broker" $Runtime
        Exe       = "surgewave-broker.exe"
        ExtraArgs = if ($PostgreSql) { @("--Surgewave:PostgreSql:Enabled=true") } else { @() }
        Color     = "Cyan"
    }
    @{
        Name      = "Gateway"
        Skip      = $SkipGateway
        Dir       = Join-Path $appsDir "gateway" $Runtime
        Exe       = "surgewave-gateway.exe"
        ExtraArgs = @()
        Color     = "Magenta"
    }
    @{
        Name      = "Control"
        Skip      = $SkipControl
        Dir       = Join-Path $appsDir "control" $Runtime
        Exe       = "surgewave-control.exe"
        ExtraArgs = @()
        Color     = "Green"
    }
    @{
        Name      = "Marketplace"
        Skip      = $SkipMarketplace
        Dir       = Join-Path $appsDir "marketplace" $Runtime
        Exe       = "surgewave-marketplace.exe"
        ExtraArgs = @()
        Color     = "Yellow"
    }
)
Write-Host ""
Write-Host "  Surgewave — Start Published" -ForegroundColor Cyan
Write-Host "  Runtime: $Runtime" -ForegroundColor Gray
Write-Host ""
# ── Validate executables exist ──────────────────────────────────────────────
$missing = @()
foreach ($svc in $services) {
    if ($svc.Skip) { continue }
    $exePath = Join-Path $svc.Dir $svc.Exe
    if (-not (Test-Path $exePath)) {
        $missing += "$($svc.Name): $exePath"
    }
}
if ($missing.Count -gt 0) {
    Write-Host "  ERROR: published executables missing:" -ForegroundColor Red
    foreach ($m in $missing) { Write-Host "    - $m" -ForegroundColor Red }
    Write-Host ""
    Write-Host "  Run .\scripts\publish.ps1 first." -ForegroundColor Yellow
    Write-Host ""
    exit 1
}
# ── Launch each service in its own console window ───────────────────────────
$launched = @()
foreach ($svc in $services) {
    if ($svc.Skip) {
        Write-Host "  $($svc.Name)... skipped" -ForegroundColor DarkGray
        continue
    }
    $exePath = Join-Path $svc.Dir $svc.Exe
    # Quote args that contain spaces
    $argList = @()
    foreach ($a in $svc.ExtraArgs) {
        if ($a -match '\s') { $argList += "`"$a`"" } else { $argList += $a }
    }
    Write-Host "  $($svc.Name)... " -NoNewline
    try {
        $proc = Start-Process `
            -FilePath $exePath `
            -ArgumentList $argList `
            -WorkingDirectory $svc.Dir `
            -PassThru `
            -WindowStyle Normal
        $launched += @{ Name = $svc.Name; Process = $proc; Color = $svc.Color }
        Write-Host "started (PID $($proc.Id))" -ForegroundColor $svc.Color
    } catch {
        Write-Host "FAILED: $_" -ForegroundColor Red
    }
}
Write-Host ""
if ($launched.Count -eq 0) {
    Write-Host "  No services started." -ForegroundColor Yellow
    exit 1
}
# ── Print connection summary ────────────────────────────────────────────────
Write-Host "  Endpoints:" -ForegroundColor Yellow
foreach ($svc in $launched) {
    switch ($svc.Name) {
        "Broker" {
            Write-Host "    Native protocol     tcp://localhost:9091" -ForegroundColor Gray
            Write-Host "    Kafka protocol      tcp://localhost:9092" -ForegroundColor Gray
            Write-Host "    gRPC                http://localhost:5095" -ForegroundColor Gray
            if ($PostgreSql) {
                Write-Host "    PostgreSQL wire     tcp://localhost:5432  (psql -h localhost -p 5432)" -ForegroundColor Gray
            }
        }
        "Gateway" {
            Write-Host "    Gateway HTTP        http://localhost:8082" -ForegroundColor Gray
        }
        "Control" {
            Write-Host "    Control UI          $ControlUrl" -ForegroundColor Gray
        }
        "Marketplace" {
            Write-Host "    Marketplace HTTP    $MarketplaceUrl" -ForegroundColor Gray
        }
    }
}
Write-Host ""
# ── Add surgewave CLI to PATH (session-only; needs dot-sourcing) ────────────────
if (-not $SkipCli) {
    $cliDir = Join-Path $appsDir "cli" $Runtime
    $cliExe = Join-Path $cliDir "surgewave.exe"
    if (Test-Path $cliExe) {
        $isDotSourced = $MyInvocation.InvocationName -eq '.'
        $alreadyInPath = ($env:PATH -split [IO.Path]::PathSeparator) -contains $cliDir
        if ($isDotSourced) {
            if (-not $alreadyInPath) {
                $env:PATH = $cliDir + [IO.Path]::PathSeparator + $env:PATH
                Write-Host "  surgewave CLI added to PATH for this session:" -ForegroundColor Yellow
                Write-Host "    $cliDir" -ForegroundColor Gray
                Write-Host "  Try: surgewave --help" -ForegroundColor Gray
            } else {
                Write-Host "  surgewave CLI already in PATH: $cliDir" -ForegroundColor DarkGray
            }
            Write-Host ""
        } else {
            Write-Host "  surgewave CLI: dot-source this script to add it to PATH for this session:" -ForegroundColor Yellow
            Write-Host "    . .\scripts\start.ps1" -ForegroundColor Gray
            Write-Host "  Or add it manually:" -ForegroundColor DarkGray
            Write-Host "    `$env:PATH = '$cliDir' + [IO.Path]::PathSeparator + `$env:PATH" -ForegroundColor DarkGray
            Write-Host ""
        }
    }
}
# ── Optional wait mode ──────────────────────────────────────────────────────
if ($Wait) {
    Write-Host "  Press Ctrl+C to stop all services and exit..." -ForegroundColor Yellow
    try {
        while ($true) {
            Start-Sleep -Seconds 1
            $alive = $launched | Where-Object { -not $_.Process.HasExited }
            if ($alive.Count -eq 0) {
                Write-Host "  All services exited." -ForegroundColor Yellow
                break
            }
        }
    } finally {
        Write-Host ""
        Write-Host "  Stopping services..." -ForegroundColor Yellow
        foreach ($svc in $launched) {
            if (-not $svc.Process.HasExited) {
                Write-Host "    $($svc.Name) (PID $($svc.Process.Id))..." -NoNewline
                try {
                    $svc.Process.CloseMainWindow() | Out-Null
                    if (-not $svc.Process.WaitForExit(5000)) {
                        $svc.Process.Kill()
                    }
                    Write-Host " stopped" -ForegroundColor Green
                } catch {
                    Write-Host " kill failed: $_" -ForegroundColor Red
                }
            }
        }
        Write-Host ""
    }
} else {
    Write-Host "  Services running in separate windows. Close the windows or kill the PIDs to stop." -ForegroundColor Gray
    Write-Host ""
}
