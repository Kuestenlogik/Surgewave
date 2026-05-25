# chocolateyInstall.ps1 for Surgewave
#
# Downloads the per-machine x64 MSI from the matching GitHub release,
# verifies it against the SHA256 baked in by the release pipeline,
# then runs `msiexec /qn` to install. The MSI itself adds the
# install folder to system PATH, registers the broker as a Windows
# service, and creates Start Menu shortcuts — so after install,
# `surgewave` works in any new shell and the Control UI is reachable
# at http://localhost:5050.
#
# Placeholders:
#   __VERSION__   — patched by chocolatey.yml workflow before pack
#   __SHA256__    — SHA256 of surgewave-<version>-win-x64.msi from the release
#
# Why x64-only: ARM64 Windows users are <1% of the install base. If
# demand picks up we can add an arch-detection branch here using
# `Get-CimInstance Win32_Processor`.

$ErrorActionPreference = 'Stop'

$packageName  = 'surgewave'
$version      = '__VERSION__'
$url64        = "https://github.com/Kuestenlogik/Surgewave/releases/download/v$version/surgewave-$version-win-x64.msi"
$checksum64   = '__SHA256__'

$packageArgs = @{
    packageName    = $packageName
    fileType       = 'msi'
    url64bit       = $url64
    checksum64     = $checksum64
    checksumType64 = 'sha256'
    softwareName   = 'Surgewave*'
    silentArgs     = '/qn /norestart'
    validExitCodes = @(0, 3010, 1641)
}

Install-ChocolateyPackage @packageArgs
