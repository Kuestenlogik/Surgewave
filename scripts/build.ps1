#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Compiles the Surgewave solution and produces NuGet packages.
.DESCRIPTION
    Two phases:
      1. dotnet build  Kuestenlogik.Surgewave.slnx -c <Configuration>
      2. dotnet pack   Kuestenlogik.Surgewave.slnx -c <Configuration> --no-build
         → artifacts/pkg/*.nupkg
    Use this when you need fresh NuGet packages — for example before
    rebuilding the Samples solution which consumes them via the local
    `surgewave-local` feed.
    For self-contained executables (Broker, Gateway, Control, …) use
    `publish.ps1` instead. The two scripts are independent: `publish.ps1`
    runs its own implicit build, so you only need `build.ps1` when you
    actually want the .nupkg artifacts.
.PARAMETER Configuration
    Build configuration (default: Release).
.PARAMETER SkipPack
    Build only, do not produce NuGet packages.
.EXAMPLE
    .\scripts\build.ps1
.EXAMPLE
    .\scripts\build.ps1 -Configuration Debug -SkipPack
#>
[CmdletBinding()]
param(
    [string]$Configuration = "Release",
    [switch]$SkipPack
)
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$slnx = Join-Path $root "Kuestenlogik.Surgewave.slnx"
$nugetDir = Join-Path $root "artifacts" "pkg"
Write-Host ""
Write-Host "  Surgewave — Build" -ForegroundColor Cyan
Write-Host "  Configuration: $Configuration" -ForegroundColor Gray
Write-Host "  Solution:      $slnx" -ForegroundColor Gray
Write-Host ""
if (-not (Test-Path $slnx)) {
    Write-Host "  ERROR: solution file not found: $slnx" -ForegroundColor Red
    exit 1
}
$failed = 0
$sw = [System.Diagnostics.Stopwatch]::StartNew()
# ── Build ───────────────────────────────────────────────────────────────────
Write-Host "━━━ Build ━━━" -ForegroundColor Yellow
Write-Host "  Building solution..." -NoNewline
dotnet build $slnx -c $Configuration --nologo -v quiet 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host " FAILED" -ForegroundColor Red
    Write-Host ""
    Write-Host "  Re-run with verbose output to inspect errors:" -ForegroundColor Yellow
    Write-Host "    dotnet build $slnx -c $Configuration" -ForegroundColor Gray
    exit 1
}
Write-Host " OK" -ForegroundColor Green
Write-Host ""
# ── Pack ────────────────────────────────────────────────────────────────────
if (-not $SkipPack) {
    Write-Host "━━━ NuGet Packages ━━━" -ForegroundColor Yellow
    Write-Host "  Packing..." -NoNewline
    dotnet pack $slnx -c $Configuration --no-build --nologo -v quiet 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host " FAILED" -ForegroundColor Red
        $failed++
    } else {
        $count = (Get-ChildItem -Path $nugetDir -Filter "*.nupkg" -ErrorAction SilentlyContinue).Count
        Write-Host " $count packages → $nugetDir" -ForegroundColor Green
    }
    Write-Host ""
}
# ── Summary ─────────────────────────────────────────────────────────────────
$sw.Stop()
$elapsed = "{0:mm}:{0:ss}" -f $sw.Elapsed
if ($failed -gt 0) {
    Write-Host "  $failed step(s) failed (elapsed $elapsed)" -ForegroundColor Red
    exit 1
} else {
    Write-Host "  Build done (elapsed $elapsed)" -ForegroundColor Green
}
Write-Host ""
