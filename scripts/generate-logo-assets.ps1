#requires -Version 5.1
# Generate the full Surgewave logo asset set, mirroring the Küstenlogik
# convention in C:\Projekte\Kuestenlogik\Brand\assets:
#
#   mark_<variant>.svg / .png / @2x.png        — icon only
#   surgewave_lockup_h_<variant>.svg / .png / @2x  — mark + Surgewave right
#   surgewave_lockup_v_<variant>.svg / .png / @2x  — mark + Surgewave below
#
# Variants:
#   colored   — full brand palette (#33bcff cyan, #66cfff cyan-light, #003e60 navy)
#   white     — every fill/stroke = #ffffff (for dark backdrops)
#   black     — every fill/stroke = #000000 (for press, mono prints)
#   adaptive  — CSS classes + prefers-color-scheme media query so the
#               same file renders correctly on both light and dark.
#
# Outputs land in C:\Projekte\Kuestenlogik\Surgewave\images\  and are
# copied into the served paths (site/assets/images, docs/templates/...)
# by the build script.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$imagesDir = Join-Path $root 'images'
$chrome = 'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe'

# ----- Mark paths (lifted from images/surgewave_logo.svg) -----
# Two semantic groups so the adaptive variant can swap them per theme:
#   wave  = the cyan wind/cloud strokes (path41-2 + path56-5)
#   ink   = the navy body strokes      (path41-2-2 + path58 + path56)
$markPaths = @{
  Wave1 = 'm 95.130627,56.386739 c -5.389529,0 -9.820478,4.387905 -9.891385,9.761678 h 5.000728 c 0.06777,-2.668882 2.203414,-4.761466 4.890657,-4.761466 2.731013,-1e-6 4.890653,2.161195 4.890653,4.892208 0,2.71208 -2.129671,4.860481 -4.833808,4.89014 l -24.548205,-0.08733 c 0.700506,2.052312 0.851744,3.555356 0.944794,5.000212 13.358013,0.01054 10.39167,0.08476 23.749674,0.08785 v -0.0021 c 5.340655,-0.109535 9.687785,-4.52352 9.687785,-9.888802 0,-5.433214 -4.45766,-9.892421 -9.890874,-9.89242 z'
  Wave2 = 'm 43.366914,75.22779 12.11278,-0.004 c -5.15629,-4.52398 -8.2055,-13.17912 -2.1496,-21.13713 5.016823,-6.59279 14.428372,-7.87017 21.020942,-2.85305 0.0701,0.0535 0.13966,0.10756 0.20877,0.16226 l 6.05648,-7.95817 c -8.59795,-6.70306 -25.213102,-8.58791 -35.243842,4.593 -5.98969,7.87085 -6.77593,18.533 -2.00557,27.19731 z'
  Ink1  = 'm 95.08012,115.91586 c -5.389523,0 -9.820472,-4.3879 -9.891379,-9.76168 h 5.000728 c 0.06777,2.66888 2.203414,4.76147 4.890651,4.76147 2.731005,0 4.890633,-2.1612 4.890633,-4.89221 0,-2.71208 -2.129658,-4.86049 -4.833783,-4.89015 l -12.688352,0.0464 c 0.86524,-1.66674 1.124339,-3.333478 1.23251,-5.000215 l 11.602099,-0.04481 c 5.340633,0.109535 9.687763,4.523525 9.687763,9.888815 0,5.43321 -4.45766,9.89242 -9.89085,9.89242 z'
  Ink2  = 'm 87.551243,55.433307 c -5.389529,0 -9.820478,4.387905 -9.891385,9.761678 h 5.000728 c 0.06777,-2.668882 2.203414,-4.761466 4.890657,-4.761466 2.731013,-10e-7 4.890658,2.161194 4.890658,4.892207 0,2.71208 -2.129676,4.860482 -4.833813,4.890141 l -30.222084,-0.05012 c 1.12428,1.688192 2.831807,3.352081 5.264824,4.985742 0,0 9.059641,0.02565 12.219552,0.03876 3.1141,0.01292 6.253737,0.02482 12.883968,0.02635 v -0.0021 c 5.340655,-0.109535 9.687787,-4.52352 9.687787,-9.888802 0,-5.433214 -4.457662,-9.89242 -9.890876,-9.892419 z'
  Ink3  = 'm 77.516199,78.89441 -11.253697,0.0042 c 5.156293,4.52398 7.346412,13.179124 1.290515,21.13713 -5.016824,6.59279 -14.42837,7.87017 -21.02094,2.85305 -0.07007,-0.0535 -0.139659,-0.10756 -0.208772,-0.16226 l -6.056478,7.95817 c 8.597942,6.70306 25.213099,8.58791 35.243843,-4.593 5.989691,-7.870853 6.77593,-18.532996 2.005563,-27.197308 z'
}

# Wave1 sits inside an inner translate(12.27173, 11.95761); compose it into
# the outer translate so every variant uses the same single transform.
$wave1Translate = 'translate(-27.995097,-26.285107)'   # = (-40.267 + 12.272, -38.243 + 11.958)
$bodyTranslate  = 'translate(-40.266827,-38.242717)'   # outer layer1 translate

function Build-MarkBody([string]$WaveColor1, [string]$WaveColor2, [string]$InkColor, [bool]$Adaptive) {
  if ($Adaptive) {
    $wave1Cls = 'class="surgewave-wave"';   $wave2Cls = 'class="surgewave-wave"'
    $ink1Cls  = 'class="surgewave-ink"';    $ink2Cls  = 'class="surgewave-ink"'; $ink3Cls = 'class="surgewave-ink"'
    $fillW1=''; $fillW2=''; $fillI1=''; $fillI2=''; $fillI3=''
  } else {
    $wave1Cls = ''; $wave2Cls = ''; $ink1Cls = ''; $ink2Cls = ''; $ink3Cls = ''
    $fillW1 = "fill=`"$WaveColor1`""
    $fillW2 = "fill=`"$WaveColor2`""
    $fillI1 = "fill=`"$InkColor`""
    $fillI2 = "fill=`"$InkColor`""
    $fillI3 = "fill=`"$InkColor`""
  }
  $w1 = $markPaths.Wave1; $w2 = $markPaths.Wave2
  $i1 = $markPaths.Ink1;  $i2 = $markPaths.Ink2;  $i3 = $markPaths.Ink3
  return @"
  <g transform="$bodyTranslate">
    <g transform="translate(12.27173,11.95761)"><path $wave1Cls $fillW1 d="$w1" /></g>
    <path $ink1Cls $fillI1 d="$i1" />
    <path $ink2Cls $fillI2 d="$i2" />
    <path $ink3Cls $fillI3 d="$i3" />
    <path $wave2Cls $fillW2 d="$w2" />
  </g>
"@
}

function Adaptive-Style([string]$WaveLight, [string]$WaveLight2, [string]$InkLight, [string]$WaveDark, [string]$WaveDark2, [string]$InkDark, [string]$TextLight, [string]$TextLightLite, [string]$TextDark, [string]$TextDarkLite) {
  return @"
    .surgewave-wave { fill: $WaveLight; }
    .surgewave-ink  { fill: $InkLight; }
    .surgewave-text-bold  { fill: $TextLight; font-family: Inter, system-ui, sans-serif; font-weight: 700; }
    @media (prefers-color-scheme: dark) {
      .surgewave-wave { fill: $WaveDark; }
      .surgewave-ink  { fill: $WaveDark2; }   /* keep contrast on dark; navy goes white */
      .surgewave-text-bold { fill: $TextDark; }
    }
    /* Manual toggle on the parent document (only effective when SVG inlined). */
    :root[data-theme-resolved="dark"] .surgewave-wave,
    [data-theme-resolved="dark"] .surgewave-wave { fill: $WaveDark; }
    :root[data-theme-resolved="dark"] .surgewave-ink,
    [data-theme-resolved="dark"] .surgewave-ink { fill: $WaveDark2; }
    :root[data-theme-resolved="dark"] .surgewave-text-bold,
    [data-theme-resolved="dark"] .surgewave-text-bold { fill: $TextDark; }
"@
}

# ============================================================
# Variants
# ============================================================
$variants = @(
  @{ name='colored'; wave1='#33bcff'; wave2='#66cfff'; ink='#003e60'; text='#003e60'; adaptive=$false },
  @{ name='white';   wave1='#ffffff'; wave2='#ffffff'; ink='#ffffff'; text='#ffffff'; adaptive=$false },
  @{ name='black';   wave1='#000000'; wave2='#000000'; ink='#000000'; text='#000000'; adaptive=$false }
)

# ============================================================
# Mark only — square 77×77
# ============================================================
function Write-Mark($variant) {
  $name = $variant.name
  $body = Build-MarkBody $variant.wave1 $variant.wave2 $variant.ink $variant.adaptive
  $svg = @"
<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 77.026421 77.673187" role="img" aria-label="Surgewave mark">
$body
</svg>
"@
  $path = Join-Path $imagesDir "mark_$name.svg"
  [System.IO.File]::WriteAllText($path, $svg)
  Write-Host "  wrote $path"
}
function Write-MarkAdaptive() {
  # Use surgewave-wave / surgewave-ink classes; CSS swaps via prefers-color-scheme.
  $body = Build-MarkBody '' '' '' $true
  $style = Adaptive-Style '#33bcff' '#66cfff' '#003e60' '#66cfff' '#ffffff' '#ffffff' '#003e60' '#33bcff' '#ffffff' '#66cfff'
  $svg = @"
<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 77.026421 77.673187" role="img" aria-label="Surgewave mark (theme-aware)">
  <defs><style>
$style
  </style></defs>
$body
</svg>
"@
  $path = Join-Path $imagesDir "mark_adaptive.svg"
  [System.IO.File]::WriteAllText($path, $svg)
  Write-Host "  wrote $path"
}

# ============================================================
# Lockups: <use href="..."> referencing the matching mark variant
# inline (so the file is self-contained — no external dependency).
# ============================================================
function Build-LockupSymbol([string]$Color1, [string]$Color2, [string]$InkColor, [bool]$Adaptive) {
  $body = Build-MarkBody $Color1 $Color2 $InkColor $Adaptive
  return @"
    <symbol id="surgewave-mark" viewBox="0 0 77.026421 77.673187">
      $body
    </symbol>
"@
}

function Write-LockupH($variant) {
  $name = $variant.name
  $sym = Build-LockupSymbol $variant.wave1 $variant.wave2 $variant.ink $variant.adaptive
  $textFill = if ($variant.adaptive) { 'class="surgewave-text-bold"' } else { "fill=`"$($variant.text)`"" }
  $style = ''
  if ($variant.adaptive) {
    $style = Adaptive-Style '#33bcff' '#66cfff' '#003e60' '#66cfff' '#ffffff' '#ffffff' '#003e60' '#33bcff' '#ffffff' '#66cfff'
  }
  $svg = @"
<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1100 200" role="img" aria-label="Surgewave logo horizontal">
  <defs>
    <style>@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700&amp;display=swap');
$style
    </style>
$sym
  </defs>
  <use href="#surgewave-mark" x="10" y="10" width="180" height="180" />
  <text x="220" y="100" font-family="Inter, system-ui, sans-serif" font-size="100" font-weight="700" letter-spacing="0" dominant-baseline="middle" $textFill>Surgewave</text>
</svg>
"@
  $path = Join-Path $imagesDir "surgewave_lockup_h_$name.svg"
  [System.IO.File]::WriteAllText($path, $svg)
  Write-Host "  wrote $path"
}

function Write-LockupV($variant) {
  $name = $variant.name
  $sym = Build-LockupSymbol $variant.wave1 $variant.wave2 $variant.ink $variant.adaptive
  $textFill = if ($variant.adaptive) { 'class="surgewave-text-bold"' } else { "fill=`"$($variant.text)`"" }
  $style = ''
  if ($variant.adaptive) {
    $style = Adaptive-Style '#33bcff' '#66cfff' '#003e60' '#66cfff' '#ffffff' '#ffffff' '#003e60' '#33bcff' '#ffffff' '#66cfff'
  }
  $svg = @"
<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 480 380" role="img" aria-label="Surgewave logo vertical">
  <defs>
    <style>@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700&amp;display=swap');
$style
    </style>
$sym
  </defs>
  <use href="#surgewave-mark" x="120" y="20" width="240" height="240" />
  <text x="240" y="340" font-family="Inter, system-ui, sans-serif" font-size="70" font-weight="700" letter-spacing="0" text-anchor="middle" $textFill>Surgewave</text>
</svg>
"@
  $path = Join-Path $imagesDir "surgewave_lockup_v_$name.svg"
  [System.IO.File]::WriteAllText($path, $svg)
  Write-Host "  wrote $path"
}

# ============================================================
# Adaptive lockup factories
# ============================================================
function Write-LockupHAdaptive() {
  $variant = @{ name='adaptive'; wave1=''; wave2=''; ink=''; text=''; adaptive=$true }
  Write-LockupH $variant
}
function Write-LockupVAdaptive() {
  $variant = @{ name='adaptive'; wave1=''; wave2=''; ink=''; text=''; adaptive=$true }
  Write-LockupV $variant
}

# ============================================================
# PNG rasterisation via Edge headless (1× and 2×, transparent bg)
# ============================================================
function Render-Png([string]$svgPath, [string]$pngPath, [int]$width, [int]$height) {
  if (Test-Path $pngPath) { Remove-Item $pngPath }
  $tmp = Join-Path $env:TEMP "edge-tmp-$(Get-Random)"
  # Edge writes "N bytes written to file ..." to stderr on success.
  # With $ErrorActionPreference = 'Stop' (set at the top of this script)
  # PowerShell turns that stderr-line into an error record and halts.
  # Drop EAP to Continue around the call so the success-notice is just
  # noise, not a fatal record.
  $prevEAP = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  try {
    & $chrome --headless --disable-gpu --no-sandbox --hide-scrollbars `
      --user-data-dir="$tmp" --window-size=$width,$height `
      --screenshot="$pngPath" --default-background-color=00000000 `
      "file:///$($svgPath.Replace('\','/'))" 2>&1 | Out-Null
  } finally {
    $ErrorActionPreference = $prevEAP
  }
  Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
  if (-not (Test-Path $pngPath)) {
    throw "Render-Png failed: $pngPath not produced (svg=$svgPath)"
  }
}

# ============================================================
# Driver
# ============================================================
Write-Host "==> Marks"
foreach ($v in $variants) { Write-Mark $v }
Write-MarkAdaptive

Write-Host "==> Horizontal lockups"
foreach ($v in $variants) { Write-LockupH $v }
Write-LockupHAdaptive

Write-Host "==> Vertical lockups"
foreach ($v in $variants) { Write-LockupV $v }
Write-LockupVAdaptive

Write-Host "==> PNGs (1× + 2×)"
$pngTargets = @(
  @{ svg='mark_colored';        size=512 },
  @{ svg='mark_white';          size=512 },
  @{ svg='mark_black';          size=512 },
  @{ svg='surgewave_lockup_h_colored'; w=1100; h=200 },
  @{ svg='surgewave_lockup_h_white';   w=1100; h=200 },
  @{ svg='surgewave_lockup_h_black';   w=1100; h=200 },
  @{ svg='surgewave_lockup_v_colored'; w=480; h=380 },
  @{ svg='surgewave_lockup_v_white';   w=480; h=380 },
  @{ svg='surgewave_lockup_v_black';   w=480; h=380 }
)
foreach ($t in $pngTargets) {
  $svg = Join-Path $imagesDir "$($t.svg).svg"
  if ($t.size) { $w = $t.size; $h = $t.size } else { $w = $t.w; $h = $t.h }
  Render-Png $svg (Join-Path $imagesDir "$($t.svg).png")    $w     $h
  Render-Png $svg (Join-Path $imagesDir "$($t.svg)@2x.png") ($w*2) ($h*2)
  Write-Host "  $($t.svg).png + @2x"
}

Write-Host ""
Write-Host "Done. $((Get-ChildItem $imagesDir -Filter 'mark_*' | Measure-Object).Count + (Get-ChildItem $imagesDir -Filter 'surgewave_lockup_*' | Measure-Object).Count) asset files in $imagesDir"
