#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Automated test runner for Surgewave Kafka wire-compatibility tests

.DESCRIPTION
    This script automates the process of:
    1. Starting the Surgewave broker
    2. Waiting for it to be ready
    3. Running the Confluent Kafka compatibility tests
    4. Cleaning up by stopping the broker

.PARAMETER SkipBuild
    Skip building the solution before running tests

.PARAMETER BrokerPort
    The port the Surgewave broker should listen on (default: 9092)

.PARAMETER TestFilter
    Test filter pattern (default: ConfluentKafkaCompatibility)

.PARAMETER ShowDetails
    Show detailed test output

.EXAMPLE
    .\run-integration-tests.ps1

.EXAMPLE
    .\run-integration-tests.ps1 -ShowDetails

.EXAMPLE
    .\run-integration-tests.ps1 -SkipBuild -BrokerPort 9093
#>

[CmdletBinding()]
param(
    [switch]$SkipBuild,
    [int]$BrokerPort = 9092,
    [string]$TestFilter = "ConfluentKafkaCompatibility",
    [switch]$ShowDetails
)

$ErrorActionPreference = "Stop"
$OriginalLocation = Get-Location

# Set UTF-8 encoding for console output
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# Colors for output
function Write-Step { param($Message) Write-Host "===> $Message" -ForegroundColor Cyan }
function Write-Success { param($Message) Write-Host "[OK] $Message" -ForegroundColor Green }
function Write-Error { param($Message) Write-Host "[ERROR] $Message" -ForegroundColor Red }
function Write-Warning { param($Message) Write-Host "[WARN] $Message" -ForegroundColor Yellow }

# Get script directory and project root
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$BrokerProject = Join-Path $ProjectRoot "src\Kuestenlogik.Surgewave.Broker\Kuestenlogik.Surgewave.Broker.csproj"
$TestProject = Join-Path $ProjectRoot "tests\Kuestenlogik.Surgewave.Tests\Kuestenlogik.Surgewave.Tests.csproj"

# Cleanup function
$BrokerProcess = $null
function Cleanup {
    Write-Step "Cleaning up..."

    if ($BrokerProcess -and !$BrokerProcess.HasExited) {
        Write-Step "Stopping Surgewave broker (PID: $($BrokerProcess.Id))..."
        try {
            Stop-Process -Id $BrokerProcess.Id -Force -ErrorAction SilentlyContinue
            Write-Success "Broker stopped"
        }
        catch {
            Write-Warning "Failed to stop broker: $_"
        }
    }

    Set-Location $OriginalLocation
}

# Register cleanup on exit
trap {
    Cleanup
    throw
}

# Main script
try {
    Write-Host ""
    Write-Host "================================================================" -ForegroundColor Cyan
    Write-Host "  Surgewave Kafka Wire-Compatibility Integration Test Runner" -ForegroundColor Cyan
    Write-Host "================================================================" -ForegroundColor Cyan
    Write-Host ""

    # Verify project files exist
    if (!(Test-Path $BrokerProject)) {
        throw "Broker project not found at: $BrokerProject"
    }
    if (!(Test-Path $TestProject)) {
        throw "Test project not found at: $TestProject"
    }

    # Step 1: Build solution (unless skipped)
    if (!$SkipBuild) {
        Write-Step "Building solution..."
        Set-Location $ProjectRoot

        $buildOutput = dotnet build --configuration Release 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Build failed!"
            Write-Host $buildOutput
            throw "Build failed with exit code $LASTEXITCODE"
        }
        Write-Success "Build completed"
    }
    else {
        Write-Warning "Skipping build (--SkipBuild specified)"
    }

    # Step 2: Check if port is available
    Write-Step "Checking if port $BrokerPort is available..."
    $portInUse = Get-NetTCPConnection -LocalPort $BrokerPort -ErrorAction SilentlyContinue
    if ($portInUse) {
        Write-Error "Port $BrokerPort is already in use!"
        Write-Host "Please stop the process using this port or specify a different port with -BrokerPort"
        throw "Port $BrokerPort is already in use"
    }
    Write-Success "Port $BrokerPort is available"

    # Step 3: Start Surgewave broker
    Write-Step "Starting Surgewave broker on port $BrokerPort..."

    $brokerArgs = @(
        "run",
        "--project", $BrokerProject,
        "--configuration", "Release",
        "--no-build"
    )

    if ($SkipBuild) {
        # Remove --no-build if we didn't build
        $brokerArgs = $brokerArgs | Where-Object { $_ -ne "--no-build" }
    }

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "dotnet"
    $psi.Arguments = $brokerArgs -join " "
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.CreateNoWindow = $true
    $psi.WorkingDirectory = $ProjectRoot

    $BrokerProcess = New-Object System.Diagnostics.Process
    $BrokerProcess.StartInfo = $psi

    # Capture output
    $outputBuilder = New-Object System.Text.StringBuilder
    $errorBuilder = New-Object System.Text.StringBuilder

    $outputHandler = {
        if ($EventArgs.Data) {
            [void]$Event.MessageData.AppendLine($EventArgs.Data)
            Write-Verbose "BROKER: $($EventArgs.Data)"
        }
    }

    $outputEvent = Register-ObjectEvent -InputObject $BrokerProcess `
        -EventName OutputDataReceived `
        -Action $outputHandler `
        -MessageData $outputBuilder

    $errorEvent = Register-ObjectEvent -InputObject $BrokerProcess `
        -EventName ErrorDataReceived `
        -Action $outputHandler `
        -MessageData $errorBuilder

    [void]$BrokerProcess.Start()
    $BrokerProcess.BeginOutputReadLine()
    $BrokerProcess.BeginErrorReadLine()

    Write-Success "Broker started (PID: $($BrokerProcess.Id))"

    # Step 4: Wait for broker to be ready
    Write-Step "Waiting for broker to be ready..."

    $maxWaitSeconds = 30
    $waitInterval = 1
    $elapsed = 0
    $brokerReady = $false

    while ($elapsed -lt $maxWaitSeconds) {
        Start-Sleep -Seconds $waitInterval
        $elapsed += $waitInterval

        # Check if broker process crashed
        if ($BrokerProcess.HasExited) {
            Write-Error "Broker process exited unexpectedly!"
            Write-Host "Exit code: $($BrokerProcess.ExitCode)"
            Write-Host "STDOUT:" -ForegroundColor Yellow
            Write-Host $outputBuilder.ToString()
            Write-Host "STDERR:" -ForegroundColor Yellow
            Write-Host $errorBuilder.ToString()
            throw "Broker crashed during startup"
        }

        # Try to connect to the broker port
        try {
            $tcpClient = New-Object System.Net.Sockets.TcpClient
            $tcpClient.Connect("localhost", $BrokerPort)
            $tcpClient.Close()
            $brokerReady = $true
            break
        }
        catch {
            Write-Host "." -NoNewline
        }
    }

    Write-Host ""

    if (!$brokerReady) {
        Write-Error "Broker did not start within $maxWaitSeconds seconds"
        Write-Host "Last output:" -ForegroundColor Yellow
        Write-Host $outputBuilder.ToString()
        throw "Broker startup timeout"
    }

    Write-Success "Broker is ready and accepting connections"

    # Step 5: Run integration tests
    Write-Step "Running integration tests..."
    Write-Host ""

    $testArgs = @(
        "test",
        $TestProject,
        "--configuration", "Release",
        "--no-build",
        "--filter", "FullyQualifiedName~$TestFilter"
    )

    if ($ShowDetails) {
        $testArgs += @("--logger", "console;verbosity=detailed")
    }

    Set-Location $ProjectRoot
    & dotnet @testArgs
    $testExitCode = $LASTEXITCODE

    Write-Host ""

    if ($testExitCode -eq 0) {
        Write-Success "All tests passed!"
    }
    else {
        Write-Error "Some tests failed (exit code: $testExitCode)"
    }

    # Step 6: Cleanup
    Cleanup

    # Final summary
    Write-Host ""
    Write-Host "================================================================" -ForegroundColor Cyan
    Write-Host "                    Test Run Summary" -ForegroundColor Cyan
    Write-Host "================================================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Broker Port:    $BrokerPort" -ForegroundColor Gray
    Write-Host "Test Filter:    $TestFilter" -ForegroundColor Gray
    if ($testExitCode -eq 0) {
        Write-Host "Exit Code:      $testExitCode (Success)" -ForegroundColor Green
    }
    else {
        Write-Host "Exit Code:      $testExitCode (Failure)" -ForegroundColor Red
    }
    Write-Host ""

    exit $testExitCode
}
catch {
    Write-Error "Script error: $($_.Exception.Message)"
    Cleanup
    exit 1
}
