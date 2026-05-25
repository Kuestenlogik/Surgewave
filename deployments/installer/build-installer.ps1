<#
.SYNOPSIS
    Builds the Surgewave Windows MSI installer.

.DESCRIPTION
    Publishes the Broker, CLI, and Control UI as self-contained win-x64 binaries,
    copies production configuration, then builds the WiX MSI package.

.PARAMETER Version
    Version number for the installer (default: 0.1.0).

.PARAMETER Configuration
    Build configuration (default: Release).

.PARAMETER SkipPublish
    Skip the dotnet publish step (use existing publish output).

.EXAMPLE
    .\build-installer.ps1
    .\build-installer.ps1 -Version 1.0.0
    .\build-installer.ps1 -SkipPublish
#>
param(
    [string]$Version = "0.1.0",
    [string]$Configuration = "Release",
    [switch]$SkipPublish
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path "$PSScriptRoot/.."
$PublishDir = "$PSScriptRoot/publish"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Surgewave MSI Installer Build" -ForegroundColor Cyan
Write-Host "  Version: $Version" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host

if (-not $SkipPublish) {
    # Clean previous publish output
    if (Test-Path $PublishDir) {
        Write-Host "[1/5] Cleaning previous publish output..." -ForegroundColor Yellow
        Remove-Item -Recurse -Force $PublishDir
    }

    # Publish Broker (self-contained, win-x64)
    Write-Host "[2/5] Publishing Surgewave Broker..." -ForegroundColor Yellow
    dotnet publish "$RepoRoot/src/Kuestenlogik.Surgewave.Broker" `
        -c $Configuration `
        -r win-x64 `
        --self-contained `
        -o "$PublishDir/broker" `
        -p:Version=$Version
    if ($LASTEXITCODE -ne 0) { throw "Broker publish failed" }

    # Publish CLI (self-contained, win-x64)
    Write-Host "[3/5] Publishing Surgewave CLI..." -ForegroundColor Yellow
    dotnet publish "$RepoRoot/src/Kuestenlogik.Surgewave.Cli" `
        -c $Configuration `
        -r win-x64 `
        --self-contained `
        -o "$PublishDir/cli" `
        -p:Version=$Version
    if ($LASTEXITCODE -ne 0) { throw "CLI publish failed" }

    # Publish Control UI (self-contained, win-x64)
    Write-Host "[4/5] Publishing Surgewave Control UI..." -ForegroundColor Yellow
    dotnet publish "$RepoRoot/src/Kuestenlogik.Surgewave.Control" `
        -c $Configuration `
        -r win-x64 `
        --self-contained `
        -o "$PublishDir/control" `
        -p:Version=$Version
    if ($LASTEXITCODE -ne 0) { throw "Control UI publish failed" }

    # Copy production config files into broker publish output
    Write-Host "     Copying production configuration..." -ForegroundColor Gray
    Copy-Item "$PSScriptRoot/config/appsettings.json" "$PublishDir/broker/appsettings.json" -Force
    Copy-Item "$PSScriptRoot/config/appsettings.Production.json" "$PublishDir/broker/appsettings.Production.json" -Force
}
else {
    Write-Host "[SKIP] Using existing publish output in $PublishDir" -ForegroundColor Gray
    if (-not (Test-Path "$PublishDir/broker")) {
        throw "No publish output found. Run without -SkipPublish first."
    }
}

# Build MSI
Write-Host "[5/5] Building MSI installer..." -ForegroundColor Yellow
$BrokerPath = (Resolve-Path "$PublishDir/broker").Path
$CliPath = (Resolve-Path "$PublishDir/cli").Path
$ControlPath = (Resolve-Path "$PublishDir/control").Path

dotnet build "$PSScriptRoot/Kuestenlogik.Surgewave.Installer/Kuestenlogik.Surgewave.Installer.wixproj" `
    -c $Configuration `
    -p:Version=$Version `
    -p:BrokerPublishDir="$BrokerPath" `
    -p:CliPublishDir="$CliPath" `
    -p:ControlPublishDir="$ControlPath"

if ($LASTEXITCODE -ne 0) { throw "MSI build failed" }

# Find the output MSI
$msiFiles = Get-ChildItem -Path "$PSScriptRoot/Kuestenlogik.Surgewave.Installer" -Recurse -Filter "*.msi" | Sort-Object LastWriteTime -Descending
if ($msiFiles.Count -gt 0) {
    $msiPath = $msiFiles[0].FullName
    Write-Host
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "  MSI built successfully!" -ForegroundColor Green
    Write-Host "  $msiPath" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
    Write-Host
    Write-Host "Install (interactive):" -ForegroundColor Cyan
    Write-Host "  msiexec /i `"$msiPath`""
    Write-Host
    Write-Host "Install (silent):" -ForegroundColor Cyan
    Write-Host "  msiexec /i `"$msiPath`" /qn"
    Write-Host
    Write-Host "Install (silent, Broker + CLI only):" -ForegroundColor Cyan
    Write-Host "  msiexec /i `"$msiPath`" /qn ADDLOCAL=BrokerFeature,CliFeature"
    Write-Host
}
else {
    Write-Host "Warning: MSI file not found in build output." -ForegroundColor Yellow
}
