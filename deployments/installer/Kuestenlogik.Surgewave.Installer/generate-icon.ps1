<#
.SYNOPSIS
    Generates surgewave.ico from the repository icon.png for the MSI installer.
    Requires System.Drawing (available on Windows with .NET Framework or .NET 6+).
#>
$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path "$PSScriptRoot/../.."
$PngPath = "$RepoRoot/icon.png"
$IcoPath = "$PSScriptRoot/surgewave.ico"

if (Test-Path $IcoPath) {
    Write-Host "surgewave.ico already exists, skipping generation."
    return
}

if (-not (Test-Path $PngPath)) {
    Write-Host "icon.png not found at $PngPath. Creating minimal ICO placeholder."
    # Minimal 16x16 ICO file (1-bit monochrome, all black = surgewave cloud)
    # ICO header: 0,0 (reserved), 1,0 (ICO type), 1,0 (1 image)
    # Directory entry: 16x16, 0 colors, 0 reserved, 1 plane, 32 bpp, data size, offset
    $bytes = [byte[]]@(
        0,0, 1,0, 1,0,  # ICO header
        16, 16, 0, 0, 1,0, 32,0,  # 16x16, 32bpp
        0x68,0x04,0,0,  # data size (1128 bytes)
        0x16,0,0,0      # data offset (22)
    )
    # BMP info header for 16x16 32bpp (40 bytes)
    $bmp = [byte[]]@(
        40,0,0,0,  # header size
        16,0,0,0,  # width
        32,0,0,0,  # height (2x for AND mask)
        1,0, 32,0, # planes=1, bpp=32
        0,0,0,0,   # compression=none
        0,0,0,0,   # image size (can be 0)
        0,0,0,0,   # x ppi
        0,0,0,0,   # y ppi
        0,0,0,0,   # colors used
        0,0,0,0    # important colors
    )
    # XOR mask: 16x16 pixels, 4 bytes each (BGRA), dark blue surgewave color
    $pixels = [byte[]]::new(16 * 16 * 4)
    for ($i = 0; $i -lt $pixels.Length; $i += 4) {
        $pixels[$i]   = 0x80  # B
        $pixels[$i+1] = 0x40  # G
        $pixels[$i+2] = 0x20  # R
        $pixels[$i+3] = 0xFF  # A
    }
    # AND mask: 16x16 bits = 16 rows of 2 bytes + 2 padding = 4 bytes/row = 64 bytes
    $andMask = [byte[]]::new(64)

    $allBytes = $bytes + $bmp + $pixels + $andMask
    # Fix data size
    $dataSize = $bmp.Length + $pixels.Length + $andMask.Length
    [BitConverter]::GetBytes([int]$dataSize).CopyTo($allBytes, 14)

    [System.IO.File]::WriteAllBytes($IcoPath, $allBytes)
    Write-Host "Placeholder surgewave.ico created."
    return
}

# Convert PNG to ICO using System.Drawing
Add-Type -AssemblyName System.Drawing
$bitmap = [System.Drawing.Bitmap]::new($PngPath)
$icon = [System.Drawing.Icon]::FromHandle($bitmap.GetHicon())
$stream = [System.IO.FileStream]::new($IcoPath, [System.IO.FileMode]::Create)
$icon.Save($stream)
$stream.Close()
$icon.Dispose()
$bitmap.Dispose()
Write-Host "surgewave.ico generated from icon.png"
