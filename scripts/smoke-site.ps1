#requires -Version 5.1
# Smoke-test the combined Jekyll + DocFX site produced by build-site.ps1.
# Runs purely against the static output in _combined/ — no HTTP server required —
# so it's safe in CI and gives deterministic failure messages.
#
# Checks performed:
#   1. All key pages exist on disk (404 → fail).
#   2. Each page is non-empty (≥ 200 bytes after Jekyll renders).
#   3. No unrendered Liquid tags or front-matter blocks leaked into output
#      ({{, {%, ---).
#   4. og:image meta tag is present + the referenced image file exists.
#   5. The 404 page exists and looks like an error page (mentions "404"
#      or "not found").
#   6. Pagefind index was generated under pagefind/.
#
# Usage:
#   .\scripts\smoke-site.ps1                # smoke-test ./_combined/
#   .\scripts\smoke-site.ps1 -CombinedDir D:\elsewhere\_combined
#   .\scripts\smoke-site.ps1 -Strict        # also fail on warnings
param(
    [string]$CombinedDir,
    [switch]$Strict
)

$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $PSScriptRoot
if (-not $CombinedDir) {
    $CombinedDir = Join-Path $Root '_combined'
}

if (-not (Test-Path $CombinedDir)) {
    throw "Combined site directory not found: $CombinedDir. Run scripts/build-site.ps1 first."
}

Write-Host "==> Smoke-testing $CombinedDir" -ForegroundColor Cyan

$errors = @()
$warnings = @()

# 1. Required pages
$RequiredPages = @(
    'index.html',
    '404.html',
    'features.html',
    'legal.html',
    'privacy.html',
    'search.html',
    'compare/kafka.html',
    'docs/index.html'
)

foreach ($rel in $RequiredPages) {
    $full = Join-Path $CombinedDir $rel
    if (-not (Test-Path $full)) {
        $errors += "MISSING  $rel"
        continue
    }
    $size = (Get-Item $full).Length
    if ($size -lt 200) {
        $errors += "TOO SMALL ($size bytes) $rel"
        continue
    }
    Write-Host "  OK $rel ($size bytes)" -ForegroundColor DarkGray
}

# 2. Liquid leakage check — anything left after Jekyll runs is a template bug
Write-Host "==> Scanning for unrendered Liquid / front matter" -ForegroundColor Cyan
$htmlFiles = Get-ChildItem -Path $CombinedDir -Recurse -Filter *.html -File
$leaks = 0
foreach ($f in $htmlFiles) {
    # Skip the docs/ subtree — DocFX templates use literal {{ in code samples.
    if ($f.FullName -like (Join-Path $CombinedDir 'docs') + '*') { continue }
    $content = Get-Content -Raw -Path $f.FullName
    $rel = $f.FullName.Substring($CombinedDir.Length + 1)
    # Check for unprocessed Liquid output / tag delimiters.
    if ($content -match '\{\{\s*[a-zA-Z_]' -or $content -match '\{%\s*[a-zA-Z_]') {
        $errors += "LIQUID LEAK $rel"
        $leaks++
    }
    # Front-matter block leaking into rendered HTML (Jekyll forgot to strip it).
    if ($content -match '^---\s*\r?\n') {
        $errors += "FRONT MATTER LEAK $rel"
    }
}
if ($leaks -eq 0) {
    Write-Host "  OK no Liquid leaks across $($htmlFiles.Count) HTML files" -ForegroundColor DarkGray
}

# 3. OG image meta tag + asset existence
Write-Host "==> Verifying og:image meta tag + referenced asset" -ForegroundColor Cyan
$index = Get-Content -Raw -Path (Join-Path $CombinedDir 'index.html')
if ($index -match 'property=["'']og:image["''][^>]*content=["'']([^"'']+)["'']') {
    $ogPath = $Matches[1]
    # Strip protocol/host if absolute.
    if ($ogPath -match '^https?://[^/]+(/.*)$') { $ogPath = $Matches[1] }
    $ogFile = Join-Path $CombinedDir $ogPath.TrimStart('/')
    if (Test-Path $ogFile) {
        $size = (Get-Item $ogFile).Length
        Write-Host "  OK og:image -> $ogPath ($size bytes)" -ForegroundColor DarkGray
    } else {
        $errors += "OG IMAGE FILE MISSING ($ogPath) — referenced by index.html but not in output"
    }
} else {
    $warnings += "og:image meta tag not found in index.html"
}

# 4. 404 page must actually look like a 404 page
Write-Host "==> Verifying 404 page content" -ForegroundColor Cyan
$notFound = Get-Content -Raw -Path (Join-Path $CombinedDir '404.html')
if ($notFound -notmatch '404|not[\s-]+found|broke before') {
    $warnings += "404.html does not mention 404 / not found — content may be wrong"
} else {
    Write-Host "  OK 404 page looks legit" -ForegroundColor DarkGray
}

# 5. Pagefind index
Write-Host "==> Verifying Pagefind index" -ForegroundColor Cyan
$pf = Join-Path $CombinedDir 'pagefind'
if (-not (Test-Path $pf)) {
    $errors += "PAGEFIND MISSING — no pagefind/ subdirectory in combined output"
} else {
    $entry = Join-Path $pf 'pagefind.js'
    if (-not (Test-Path $entry)) {
        $errors += "PAGEFIND BROKEN — pagefind/pagefind.js missing"
    } else {
        Write-Host "  OK pagefind/pagefind.js" -ForegroundColor DarkGray
    }
}

# Summary
Write-Host ''
if ($warnings.Count -gt 0) {
    Write-Host "Warnings ($($warnings.Count)):" -ForegroundColor Yellow
    foreach ($w in $warnings) { Write-Host "  $w" -ForegroundColor Yellow }
}
if ($errors.Count -gt 0) {
    Write-Host "Errors ($($errors.Count)):" -ForegroundColor Red
    foreach ($e in $errors) { Write-Host "  $e" -ForegroundColor Red }
    exit 1
}
if ($Strict -and $warnings.Count -gt 0) {
    Write-Host "Strict mode: failing on warnings." -ForegroundColor Red
    exit 1
}
Write-Host "==> Smoke test passed." -ForegroundColor Green
