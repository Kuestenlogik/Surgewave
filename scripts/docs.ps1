<#
.SYNOPSIS
    Builds the Surgewave DocFX documentation site locally.

.DESCRIPTION
    Wrapper around `docfx build` (and optionally `docfx metadata` /
    `docfx pdf` / `docfx serve`). Output goes to artifacts/docs/.
    Requires the docfx global tool: `dotnet tool install -g docfx`.

.PARAMETER Serve
    After building, start `docfx serve` on the configured port and block.

.PARAMETER Port
    Port for `-Serve` (default: 8080).

.PARAMETER Clean
    Remove artifacts/docs/ before building.

.PARAMETER WithMetadata
    Run `docfx metadata` first to regenerate API reference from source.

.PARAMETER MetadataOnly
    Only run `docfx metadata` and exit.

.PARAMETER Pdf
    After the HTML build, also produce a PDF (requires wkhtmltopdf).

.PARAMETER Tours
    Copy *.tour.json files from docs/tours/ into the output.

.EXAMPLE
    .\scripts\docs.ps1 -Serve

.EXAMPLE
    .\scripts\docs.ps1 -Clean -WithMetadata
#>
[CmdletBinding()]
param(
    [switch]$Serve,
    [int]$Port = 8080,
    [switch]$Clean,
    [switch]$WithMetadata,
    [switch]$MetadataOnly,
    [switch]$Pdf,
    [switch]$Tours
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step { param([string]$Message) Write-Host "==> $Message" -ForegroundColor Cyan }
function Write-Success { param([string]$Message) Write-Host "[OK] $Message" -ForegroundColor Green }
function Write-Err { param([string]$Message) Write-Host "[ERROR] $Message" -ForegroundColor Red }
function Write-Info { param([string]$Message) Write-Host "[INFO] $Message" -ForegroundColor Yellow }

# Resolve repo root (parent of scripts/), then docs and artifacts paths
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$docsDir = Join-Path $repoRoot "docs"
$artifactsDir = Join-Path $repoRoot "artifacts" "docs"

if (-not (Test-Path $docsDir)) {
    Write-Err "Documentation directory not found: $docsDir"
    exit 1
}

Set-Location $docsDir

Write-Step "Checking DocFX installation..."
try {
    $docfxVersion = docfx --version 2>&1 | Select-Object -First 1
    Write-Success "DocFX found: $docfxVersion"
} catch {
    Write-Err "DocFX not found. Install with: dotnet tool install -g docfx"
    exit 1
}

if ($Clean) {
    Write-Step "Cleaning documentation artifacts..."
    if (Test-Path $artifactsDir) {
        Remove-Item -Path $artifactsDir -Recurse -Force
        Write-Success "Cleaned: $artifactsDir"
    } else {
        Write-Info "No artifacts to clean"
    }
}

if ($WithMetadata -or $MetadataOnly) {
    Write-Step "Generating API metadata from source code..."
    $metadataResult = docfx metadata 2>&1
    $metadataExitCode = $LASTEXITCODE

    if ($metadataExitCode -eq 0) {
        $warningMatches = $metadataResult | Select-String "warning:"
        $warnings = if ($warningMatches) { @($warningMatches).Count } else { 0 }
        if ($warnings -gt 0) {
            Write-Success "API metadata generated with $warnings warning(s)"
        } else {
            Write-Success "API metadata generated successfully"
        }
    } else {
        Write-Err "Failed to generate API metadata"
        Write-Info "Note: API metadata generation may fail due to .NET source generators"
        $metadataResult | Write-Host
        exit 1
    }

    if ($MetadataOnly) {
        Write-Info "Metadata-only mode. Skipping site build."
        Write-Success "API metadata is ready in: $artifactsDir\api"
        exit 0
    }
} else {
    Write-Info "Skipping API metadata generation (use -WithMetadata to enable)"
}

Write-Step "Building documentation site..."
$buildResult = docfx build 2>&1
$buildExitCode = $LASTEXITCODE

if ($buildExitCode -eq 0) {
    $warningMatches = $buildResult | Select-String "warning:"
    $warnings = if ($warningMatches) { @($warningMatches).Count } else { 0 }
    $htmlFileList = Get-ChildItem -Path $artifactsDir -Filter "*.html" -Recurse -ErrorAction SilentlyContinue
    $htmlFiles = if ($htmlFileList) { @($htmlFileList).Count } else { 0 }

    Write-Success "Documentation built successfully"
    Write-Info "Generated $htmlFiles HTML files"
    if ($warnings -gt 0) {
        Write-Info "$warnings warning(s) - mostly expected (missing links, etc.)"
    }
} else {
    Write-Err "Failed to build documentation"
    $buildResult | Write-Host
    exit 1
}

Write-Success "Documentation available at: $artifactsDir"

if ($Tours) {
    Write-Step "Copying tour definitions to output..."
    $toursSource = Join-Path $docsDir "tours"
    $toursDest = Join-Path $artifactsDir "tours"
    if (Test-Path $toursSource) {
        if (-not (Test-Path $toursDest)) {
            New-Item -ItemType Directory -Path $toursDest -Force | Out-Null
        }
        Copy-Item -Path (Join-Path $toursSource "*.tour.json") -Destination $toursDest -Force
        $tourFiles = Get-ChildItem -Path $toursDest -Filter "*.tour.json" -ErrorAction SilentlyContinue
        $tourCount = if ($tourFiles) { @($tourFiles).Count } else { 0 }
        Write-Success "Copied $tourCount tour definition(s)"
    } else {
        Write-Info "No tours directory found at: $toursSource"
    }
}

if ($Pdf) {
    Write-Step "Building PDF documentation..."
    $pdfResult = docfx pdf 2>&1
    $pdfExitCode = $LASTEXITCODE
    $pdfDir = Join-Path $scriptDir "artifacts" "docs-pdf"

    if ($pdfExitCode -eq 0) {
        Write-Success "PDF documentation built successfully"
        Write-Info "PDF output: $pdfDir"
    } else {
        Write-Err "Failed to build PDF documentation"
        Write-Info "PDF generation requires wkhtmltopdf to be installed"
        $pdfResult | Write-Host
    }
}

if ($Serve) {
    Write-Step "Starting documentation server on port $Port..."
    Write-Info "Press Ctrl+C to stop the server"
    Write-Host "Documentation URL: http://localhost:$Port" -ForegroundColor Green
    docfx serve $artifactsDir -p $Port
} else {
    Write-Info "To view: docfx serve $artifactsDir -p $Port"
    Write-Info "Or run: .\scripts\docs.ps1 -Serve"
}

Write-Success "Documentation build complete!"
