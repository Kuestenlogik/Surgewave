#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Stops the published Broker, Gateway and Control services started via
    start.ps1.
.DESCRIPTION
    Finds running surgewave-broker / surgewave-gateway / surgewave-control processes
    whose executable lives under artifacts/pub/ in this repo, then
    force-stops them and waits for OS file handle release.
    Restricting the search to processes inside this repo's pub dir
    avoids accidentally stopping unrelated Surgewave instances elsewhere on
    the machine.
.PARAMETER Runtime
    Target runtime folder (default: win-x64).
.PARAMETER All
    Stop all surgewave-broker / surgewave-gateway / surgewave-control processes on
    the machine, regardless of where their executable lives.
.PARAMETER TimeoutSeconds
    How long to wait for the OS to release file handles after kill (default: 8).
.EXAMPLE
    .\scripts\stop.ps1
.EXAMPLE
    .\scripts\stop.ps1 -All
#>
[CmdletBinding()]
param(
    [string]$Runtime = "win-x64",
    [switch]$All,
    [int]$TimeoutSeconds = 8
)
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$publishDir = Join-Path $root "artifacts" "pub"
$serviceNames = @("surgewave-broker", "surgewave-gateway", "surgewave-control", "surgewave-marketplace")
Write-Host ""
Write-Host "  Surgewave — Stop Published" -ForegroundColor Cyan
if ($All) {
    Write-Host "  Scope: ALL processes (system-wide)" -ForegroundColor Yellow
} else {
    Write-Host "  Scope: $publishDir" -ForegroundColor Gray
}
Write-Host ""
# ── Discover candidate processes ─────────────────────────────────────────────
$candidates = @()
foreach ($name in $serviceNames) {
    $procs = Get-Process -Name $name -ErrorAction SilentlyContinue
    foreach ($proc in $procs) {
        $exePath = $null
        try { $exePath = $proc.Path } catch { }
        $matchesRepo = $exePath -and $exePath.StartsWith($publishDir, [StringComparison]::OrdinalIgnoreCase)
        if ($All -or $matchesRepo) {
            $candidates += [PSCustomObject]@{
                Name   = $name
                Id     = $proc.Id
                Path   = if ($exePath) { $exePath } else { "<unknown>" }
                InRepo = $matchesRepo
            }
        }
    }
}
if ($candidates.Count -eq 0) {
    Write-Host "  No matching services running." -ForegroundColor Yellow
    Write-Host ""
    exit 0
}
Write-Host "  Found $($candidates.Count) process(es):" -ForegroundColor Yellow
foreach ($c in $candidates) {
    $marker = if ($c.InRepo) { " " } else { "*" }
    Write-Host ("    {0} {1,-15} PID {2,-8} {3}" -f $marker, $c.Name, $c.Id, $c.Path) -ForegroundColor Gray
}
if (($candidates | Where-Object { -not $_.InRepo }).Count -gt 0) {
    Write-Host "    (* = outside artifacts/pub/, only stopped because -All was passed)" -ForegroundColor DarkGray
}
Write-Host ""
# ── Stop all processes ───────────────────────────────────────────────────────
Write-Host "  Stopping..." -ForegroundColor Yellow
$killed = 0
foreach ($c in $candidates) {
    try {
        Stop-Process -Id $c.Id -Force -ErrorAction Stop
        Write-Host "    $($c.Name) (PID $($c.Id)) stopped" -ForegroundColor Green
        $killed++
    } catch {
        # Process may have already exited
        if (-not (Get-Process -Id $c.Id -ErrorAction SilentlyContinue)) {
            Write-Host "    $($c.Name) (PID $($c.Id)) already exited" -ForegroundColor DarkGray
        } else {
            Write-Host "    $($c.Name) (PID $($c.Id)) stop failed: $_" -ForegroundColor Red
        }
    }
}
# Wait for OS to release file handles before returning
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$remaining = $candidates | Where-Object { Get-Process -Id $_.Id -ErrorAction SilentlyContinue }
while ($remaining.Count -gt 0 -and (Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 300
    $remaining = $candidates | Where-Object { Get-Process -Id $_.Id -ErrorAction SilentlyContinue }
}
Write-Host ""
Write-Host "  Done — $killed process(es) stopped." -ForegroundColor Green
Write-Host ""
