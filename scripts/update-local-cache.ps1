#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Replaces the locally cached NuGet copies of Surgewave packages with freshly built ones.
.DESCRIPTION
    Sibling Surgewave repos (Surgewave.Connectors, Surgewave.Ai, Surgewave.Iceberg, ...) consume
    Kuestenlogik.Surgewave.* packages via the global NuGet cache (~/.nuget/packages/<name>/<version>/).
    Without a version bump, NuGet treats the cached version as canonical and never
    re-fetches — so a freshly rebuilt artifacts/pkg/Kuestenlogik.Surgewave.Core.0.1.0.nupkg has zero
    effect on consumers until the cache is overwritten.

    This script walks every Kuestenlogik.Surgewave.*.nupkg in the package directory, locates the
    matching <name>/<version>/ folder in the NuGet cache, and overwrites it with the
    contents of the new .nupkg. The hash files (.nupkg.sha512, .nupkg.metadata) are
    rewritten with the new SHA512 so NuGet's integrity check still passes.

    Packages whose <name>/<version> is not present in the cache are skipped by default
    (use -Install to also seed them).
.PARAMETER PackageDir
    Directory to read .nupkg files from. Default: artifacts/pkg.
.PARAMETER NuGetCache
    Local NuGet cache root. Default: $env:USERPROFILE/.nuget/packages (Windows) or
    $HOME/.nuget/packages (Unix).
.PARAMETER Filter
    Wildcard filter for the package basename (without version). Example: 'Kuestenlogik.Surgewave.Core'.
    Default: 'Kuestenlogik.Surgewave.*' which matches every Surgewave package.
.PARAMETER Install
    Also create cache entries for packages that are not yet present locally. Without
    this switch, missing packages are reported and skipped.
.PARAMETER DryRun
    Print what would happen without touching the cache.
.EXAMPLE
    .\scripts\update-local-cache.ps1
.EXAMPLE
    .\scripts\update-local-cache.ps1 -Filter Kuestenlogik.Surgewave.Streams -DryRun
.EXAMPLE
    .\scripts\update-local-cache.ps1 -Install
#>
[CmdletBinding()]
param(
    [string]$PackageDir,
    [string]$NuGetCache,
    [string]$Filter = "Kuestenlogik.Surgewave.*",
    [switch]$Install,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
if (-not $PackageDir) {
    $PackageDir = Join-Path $root "artifacts" "pkg"
}
if (-not $NuGetCache) {
    $homeDir = if ($env:USERPROFILE) { $env:USERPROFILE } else { $env:HOME }
    $NuGetCache = Join-Path $homeDir ".nuget" "packages"
}

Write-Host ""
Write-Host "  Surgewave - Update local NuGet cache" -ForegroundColor Cyan
Write-Host "  package dir : $PackageDir"
Write-Host "  cache root  : $NuGetCache"
Write-Host "  filter      : $Filter"
if ($DryRun) { Write-Host "  mode        : DRY RUN (no files touched)" -ForegroundColor Yellow }
Write-Host ""

if (-not (Test-Path $PackageDir)) {
    Write-Host "  ERROR: package directory not found: $PackageDir" -ForegroundColor Red
    exit 1
}
if (-not (Test-Path $NuGetCache)) {
    Write-Host "  ERROR: NuGet cache not found: $NuGetCache" -ForegroundColor Red
    exit 1
}

# Pull only the .nupkg files (not .snupkg / .symbols.nupkg — those are symbol packages
# that NuGet stores differently and consumers do not depend on for resolution).
# A filter without an explicit wildcard is treated as a name prefix (e.g. -Filter
# 'Kuestenlogik.Surgewave.Core' should match every version of that package), so append '*' before
# the extension when the user did not include one.
$filterPattern = if ($Filter.Contains('*')) { "$Filter.nupkg" } else { "$Filter*.nupkg" }
$nupkgs = Get-ChildItem -Path $PackageDir -Filter $filterPattern |
    Where-Object { $_.Name -notlike "*.snupkg" -and $_.Name -notlike "*.symbols.nupkg" } |
    Sort-Object Name

if ($nupkgs.Count -eq 0) {
    Write-Host "  No matching .nupkg files in $PackageDir" -ForegroundColor Yellow
    exit 1
}

# Filename → name + version. Examples:
#   Kuestenlogik.Surgewave.Core.0.1.0.nupkg                       → name=Kuestenlogik.Surgewave.Core, version=0.1.0
#   Kuestenlogik.Surgewave.Streams.0.1.0-preview.4.nupkg          → name=Kuestenlogik.Surgewave.Streams, version=0.1.0-preview.4
#   Kuestenlogik.Surgewave.Plugins.Repository.1.2.3+build.5.nupkg → name=Kuestenlogik.Surgewave.Plugins.Repository, version=1.2.3+build.5
$versionRegex = '^(?<name>.+?)\.(?<version>\d+\.\d+\.\d+(?:[-+][\w.+-]+)?)$'

function Get-Sha512Base64 {
    param([string]$Path)
    $hashHex = (Get-FileHash -Path $Path -Algorithm SHA512).Hash
    $bytes = New-Object byte[] ($hashHex.Length / 2)
    for ($i = 0; $i -lt $bytes.Length; $i++) {
        $bytes[$i] = [Convert]::ToByte($hashHex.Substring($i * 2, 2), 16)
    }
    return [Convert]::ToBase64String($bytes)
}

function Replace-CacheEntry {
    param(
        [string]$NupkgPath,
        [string]$CacheDir,
        [string]$LowerNameVersionFile
    )
    # Wipe the old cache directory in full and rebuild it from the new .nupkg. We do
    # not preserve any state across the swap — the .nupkg is the source of truth and
    # NuGet only relies on the four metadata files we recreate (the .nupkg itself, its
    # .sha512 sidecar, the .nupkg.metadata JSON, and the extracted lib/ tree).
    Remove-Item -Path $CacheDir -Recurse -Force
    New-Item -ItemType Directory -Path $CacheDir -Force | Out-Null

    # Expand-Archive does not accept .nupkg directly on older PowerShells; copy to a
    # .zip-named temp file first to dodge the extension check.
    $tempZip = [System.IO.Path]::ChangeExtension(
        (Join-Path ([System.IO.Path]::GetTempPath()) ([System.IO.Path]::GetRandomFileName())),
        ".zip")
    try {
        Copy-Item -Path $NupkgPath -Destination $tempZip -Force
        Expand-Archive -Path $tempZip -DestinationPath $CacheDir -Force
    }
    finally {
        if (Test-Path $tempZip) { Remove-Item $tempZip -Force }
    }

    # Drop the new .nupkg into the cache (NuGet keeps the original archive next to
    # the extracted layout for re-use, e.g. when restoring into a different folder).
    Copy-Item -Path $NupkgPath -Destination (Join-Path $CacheDir $LowerNameVersionFile) -Force

    $hash = Get-Sha512Base64 -Path $NupkgPath

    # Sidecar hash file: NuGet's restore reads this on every package resolution.
    Set-Content -Path (Join-Path $CacheDir "$LowerNameVersionFile.sha512") -Value $hash -NoNewline -Encoding ascii

    # .nupkg.metadata: NuGet's per-package marker file. version=2 has been the format
    # since NuGet 4.x; the contentHash must match the .sha512 sidecar.
    $metadata = [pscustomobject]@{
        version     = 2
        contentHash = $hash
        source      = $PackageDir
    }
    $metadata | ConvertTo-Json -Compress | Set-Content -Path (Join-Path $CacheDir ".nupkg.metadata") -Encoding ascii
}

$replaced = 0
$installed = 0
$skipped = 0
$missing = @()

foreach ($nupkg in $nupkgs) {
    if ($nupkg.BaseName -notmatch $versionRegex) {
        Write-Host "  ! cannot parse name/version from $($nupkg.Name) — skipping" -ForegroundColor Yellow
        $skipped++
        continue
    }
    $packageName = $Matches['name']
    $version = $Matches['version']

    # NuGet cache uses lower-case package and version segment names.
    $lowerName = $packageName.ToLowerInvariant()
    $cacheDir = Join-Path $NuGetCache $lowerName $version
    $lowerNupkgFile = "$lowerName.$version.nupkg"

    if (-not (Test-Path $cacheDir)) {
        if (-not $Install) {
            $missing += "$packageName $version"
            $skipped++
            continue
        }
        if ($DryRun) {
            Write-Host "  [dry-run] would install $packageName $version" -ForegroundColor DarkCyan
            $installed++
            continue
        }
        New-Item -ItemType Directory -Path $cacheDir -Force | Out-Null
        Replace-CacheEntry -NupkgPath $nupkg.FullName -CacheDir $cacheDir -LowerNameVersionFile $lowerNupkgFile
        Write-Host "  + installed   $packageName $version" -ForegroundColor Green
        $installed++
        continue
    }

    if ($DryRun) {
        Write-Host "  [dry-run] would replace $packageName $version" -ForegroundColor DarkCyan
        $replaced++
        continue
    }

    Replace-CacheEntry -NupkgPath $nupkg.FullName -CacheDir $cacheDir -LowerNameVersionFile $lowerNupkgFile
    Write-Host "  ✓ replaced    $packageName $version" -ForegroundColor Green
    $replaced++
}

Write-Host ""
Write-Host "  Done." -ForegroundColor Cyan
Write-Host "  replaced  : $replaced"
if ($Install) {
    Write-Host "  installed : $installed"
}
Write-Host "  skipped   : $skipped"
if ($missing.Count -gt 0 -and -not $Install) {
    Write-Host ""
    Write-Host "  $($missing.Count) package(s) had no local cache entry — pass -Install to seed them:" -ForegroundColor Yellow
    foreach ($m in $missing | Select-Object -First 10) {
        Write-Host "    - $m"
    }
    if ($missing.Count -gt 10) {
        Write-Host "    ... and $($missing.Count - 10) more"
    }
}
