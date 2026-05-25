#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Runs tests with code coverage collection and generates reports.

.DESCRIPTION
    This script:
    1. Runs all unit tests with Coverlet code coverage collection
    2. Merges coverage results from all test projects
    3. Generates HTML, badge, and summary reports via ReportGenerator
    4. Optionally enforces a minimum coverage threshold
    5. Optionally opens the HTML report in a browser

.PARAMETER Threshold
    Minimum line coverage percentage (0-100). 0 disables threshold enforcement.
    If coverage falls below this value, the script exits with code 1.

.PARAMETER OpenReport
    Opens the generated HTML coverage report in the default browser.

.PARAMETER OutputDir
    Directory for coverage results and reports. Default: artifacts/coverage

.PARAMETER Format
    Coverlet output format. Default: cobertura
    Supported: cobertura, opencover, lcov, json

.PARAMETER Filter
    Test filter expression (passed to dotnet test --filter). Default: excludes integration tests.

.PARAMETER NoBuild
    Skip building before running tests.

.PARAMETER Configuration
    Build configuration. Default: Release

.EXAMPLE
    .\run-coverage.ps1
    # Run all unit tests with coverage, generate reports

.EXAMPLE
    .\run-coverage.ps1 -Threshold 60 -OpenReport
    # Run tests, enforce 60% minimum coverage, open HTML report

.EXAMPLE
    .\run-coverage.ps1 -Filter "FullyQualifiedName~Core" -NoBuild
    # Run only Core tests without rebuilding
#>

[CmdletBinding()]
param(
    [ValidateRange(0, 100)]
    [int]$Threshold = 0,

    [switch]$OpenReport,

    [string]$OutputDir = "artifacts/coverage",

    [ValidateSet("cobertura", "opencover", "lcov", "json")]
    [string]$Format = "cobertura",

    [string]$Filter = "FullyQualifiedName!~IntegrationTests",

    [switch]$NoBuild,

    [ValidateSet("Debug", "Release")]
    [string]$Configuration = "Release"
)

$ErrorActionPreference = "Stop"

# Get project root (parent of scripts/)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir

# Resolve output directory
if (-not [System.IO.Path]::IsPathRooted($OutputDir)) {
    $OutputDir = Join-Path $ProjectRoot $OutputDir
}

$RawDir = Join-Path $OutputDir "raw"
$ReportDir = Join-Path $OutputDir "report"
$RunSettings = Join-Path $ProjectRoot "coverlet.runsettings"

function Write-Step { param($Message) Write-Host "===> $Message" -ForegroundColor Cyan }
function Write-Success { param($Message) Write-Host "[OK] $Message" -ForegroundColor Green }

# Step 1: Ensure ReportGenerator is available
Write-Step "Checking for ReportGenerator tool..."

$reportGenInstalled = $false
try {
    $toolList = dotnet tool list -g 2>&1
    if ($toolList -match "dotnet-reportgenerator-globaltool") {
        $reportGenInstalled = $true
    }
}
catch { }

if (-not $reportGenInstalled) {
    # Try local tool manifest
    try {
        $toolList = dotnet tool list 2>&1
        if ($toolList -match "dotnet-reportgenerator-globaltool") {
            $reportGenInstalled = $true
        }
    }
    catch { }
}

if (-not $reportGenInstalled) {
    Write-Step "Installing ReportGenerator..."
    dotnet tool install -g dotnet-reportgenerator-globaltool 2>$null
    if ($LASTEXITCODE -ne 0) {
        # Try restoring from local manifest instead
        Push-Location $ProjectRoot
        dotnet tool restore
        Pop-Location
    }
}

Write-Success "ReportGenerator is available"

# Step 2: Clean previous results
Write-Step "Cleaning previous coverage results..."
if (Test-Path $OutputDir) {
    Remove-Item -Recurse -Force $OutputDir
}
New-Item -ItemType Directory -Force -Path $RawDir | Out-Null

# Step 3: Run tests with coverage
Write-Step "Running tests with coverage collection..."
Write-Host "  Configuration: $Configuration"
Write-Host "  Format:        $Format"
Write-Host "  Filter:        $Filter"
Write-Host "  Output:        $RawDir"
Write-Host ""

$testArgs = @(
    "test"
    (Join-Path $ProjectRoot "Kuestenlogik.Surgewave.slnx")
    "-c", $Configuration
    "--collect:XPlat Code Coverage"
    "--results-directory", $RawDir
    "--settings", $RunSettings
    "--verbosity", "minimal"
    "--filter", $Filter
    "--", "DataCollectionRunSettings.DataCollectors.DataCollector.Configuration.Format=$Format"
)

if ($NoBuild) {
    $testArgs += "--no-build"
}

& dotnet @testArgs
$testExitCode = $LASTEXITCODE

if ($testExitCode -ne 0) {
    Write-Host "[WARN] Some tests failed (exit code: $testExitCode)" -ForegroundColor Yellow
    Write-Host "       Coverage report will still be generated for tests that ran." -ForegroundColor Yellow
    Write-Host ""
}

# Step 4: Find coverage files
$coveragePattern = switch ($Format) {
    "cobertura" { "coverage.cobertura.xml" }
    "opencover" { "coverage.opencover.xml" }
    "lcov"      { "coverage.info" }
    "json"      { "coverage.json" }
}

$coverageFiles = Get-ChildItem -Recurse $RawDir -Filter $coveragePattern -ErrorAction SilentlyContinue

if ($coverageFiles.Count -eq 0) {
    Write-Host "[ERROR] No coverage files found in $RawDir" -ForegroundColor Red
    Write-Host "        Ensure tests ran successfully and coverlet.collector is referenced." -ForegroundColor Red
    exit 1
}

Write-Success "Found $($coverageFiles.Count) coverage file(s)"

# Step 5: Generate reports
Write-Step "Generating coverage reports..."

$reports = ($coverageFiles | ForEach-Object { $_.FullName }) -join ";"

$reportGenArgs = @(
    "-reports:$reports"
    "-targetdir:$ReportDir"
    "-reporttypes:Html;Badges;TextSummary;MarkdownSummaryGithub"
    "-assemblyfilters:+Kuestenlogik.Surgewave.*;-*.Tests;-*.Benchmarks"
    "-classfilters:-*.Program;-*.Startup"
)

reportgenerator @reportGenArgs
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] ReportGenerator failed" -ForegroundColor Red
    exit 1
}

Write-Success "Reports generated in $ReportDir"

# Step 6: Print summary
$summaryFile = Join-Path $ReportDir "Summary.txt"
if (Test-Path $summaryFile) {
    Write-Host ""
    Write-Host "Coverage Summary:" -ForegroundColor Cyan
    Write-Host "================================================================" -ForegroundColor Cyan
    $summaryContent = Get-Content $summaryFile
    $summaryContent | ForEach-Object { Write-Host "  $_" }
    Write-Host "================================================================" -ForegroundColor Cyan
    Write-Host ""
}

# Step 7: Check threshold
if ($Threshold -gt 0) {
    Write-Step "Checking coverage threshold ($Threshold%)..."

    $lineCoverage = 0
    if (Test-Path $summaryFile) {
        $lineMatch = $summaryContent | Select-String -Pattern "Line coverage:\s*([\d.]+)%"
        if ($lineMatch) {
            $lineCoverage = [double]$lineMatch.Matches[0].Groups[1].Value
        }
    }

    if ($lineCoverage -lt $Threshold) {
        Write-Host "[FAIL] Line coverage $lineCoverage% is below threshold $Threshold%" -ForegroundColor Red
        exit 1
    }
    else {
        Write-Success "Line coverage $lineCoverage% meets threshold $Threshold%"
    }
}

# Step 8: Open report
if ($OpenReport) {
    $indexFile = Join-Path $ReportDir "index.html"
    if (Test-Path $indexFile) {
        Write-Step "Opening coverage report..."
        Start-Process $indexFile
    }
}

# Final status
Write-Host ""
Write-Host "Coverage artifacts:" -ForegroundColor Cyan
Write-Host "  HTML Report:  $ReportDir\index.html"
Write-Host "  Summary:      $ReportDir\Summary.txt"
Write-Host "  Badge:        $ReportDir\badge_combined.svg"
Write-Host "  GitHub MD:    $ReportDir\SummaryGithub.md"
Write-Host ""

if ($testExitCode -ne 0) {
    Write-Host "Note: Some tests failed. Review test output above." -ForegroundColor Yellow
    exit $testExitCode
}

Write-Success "Coverage run completed successfully"
