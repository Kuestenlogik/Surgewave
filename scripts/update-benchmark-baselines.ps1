#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Updates benchmark baselines by running benchmarks and merging results.
.PARAMETER Category
    The benchmark category to update (default: "unit").
.EXAMPLE
    .\scripts\update-benchmark-baselines.ps1
    .\scripts\update-benchmark-baselines.ps1 -Category storage
#>
param(
    [string]$Category = "unit"
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$artifactsDir = Join-Path $repoRoot "artifacts" "benchmarks"

Write-Host "Running $Category benchmarks..." -ForegroundColor Cyan

# Map category to project
$projectMap = @{
    "unit"        = "benchmarks/Kuestenlogik.Surgewave.Benchmarks.Unit"
    "storage"     = "benchmarks/Kuestenlogik.Surgewave.Benchmarks.Storage"
    "integration" = "benchmarks/Kuestenlogik.Surgewave.Benchmarks.Integration"
    "comparison"  = "benchmarks/Kuestenlogik.Surgewave.Benchmarks.Comparison"
    "latency"     = "benchmarks/Kuestenlogik.Surgewave.Benchmarks.Latency"
    "streams"     = "benchmarks/Kuestenlogik.Surgewave.Benchmarks.Streams"
}

$project = $projectMap[$Category]
if (-not $project) {
    Write-Error "Unknown category: $Category. Valid categories: $($projectMap.Keys -join ', ')"
    exit 1
}

$projectPath = Join-Path $repoRoot $project

# Run benchmarks with JSON export
Write-Host "Building and running benchmarks from $project..." -ForegroundColor Yellow
dotnet run --project $projectPath -c Release -- --filter '*' --exporters json --job short --artifacts $artifactsDir
if ($LASTEXITCODE -ne 0) {
    Write-Error "Benchmark run failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

# Find the report file
$resultsDir = Join-Path $artifactsDir "results"
$reportFile = Get-ChildItem -Path $resultsDir -Filter "*-report-full.json" -ErrorAction SilentlyContinue | Select-Object -First 1

if (-not $reportFile) {
    Write-Error "No benchmark report file found in $resultsDir"
    exit 1
}

Write-Host "Found report: $($reportFile.FullName)" -ForegroundColor Green

# Update baseline
$baselinePath = Join-Path $repoRoot "benchmarks" "baselines" "$Category-baseline.json"
$regressionProject = Join-Path $repoRoot "benchmarks" "Kuestenlogik.Surgewave.Benchmarks.Regression"

Write-Host "Updating baseline: $baselinePath" -ForegroundColor Yellow
dotnet run --project $regressionProject -c Release -- update-baseline $reportFile.FullName $baselinePath
if ($LASTEXITCODE -ne 0) {
    Write-Error "Baseline update failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

Write-Host "Baseline updated successfully!" -ForegroundColor Green
