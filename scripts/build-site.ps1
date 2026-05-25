#requires -Version 5.1
# Local equivalent of `.github/workflows/docs.yml` — builds the combined
# Jekyll marketing site + DocFX API docs + Pagefind index in one go, with the
# same layout the CI deploy uses (Jekyll at the root, DocFX under /docs/).
#
# Usage:
#   .\scripts\build-site.ps1                # full build, output under _combined/
#   .\scripts\build-site.ps1 -SkipDotnet    # skip the .NET build (reuse last
#                                           # Release artifacts) — fast iteration
#                                           # on docs / site / template tweaks
#   .\scripts\build-site.ps1 -Serve         # full build, then host on
#                                           # http://localhost:4000 (no Jekyll
#                                           # watch — re-run the script after
#                                           # editing).
#   .\scripts\build-site.ps1 -SkipDotnet -Serve
#
# Prerequisites:
#   - .NET 10 SDK
#   - DocFX as a global dotnet tool: `dotnet tool install -g docfx`
#   - Ruby + Bundler (Jekyll). On Windows: `winget install RubyInstallerTeam.Ruby.3.3`
#     then `gem install bundler`. Inside `site/` run `bundle install` once.
#   - Node.js (any recent LTS) for Pagefind via `npx -y pagefind`.
param(
    [switch]$SkipDotnet,
    [switch]$Serve,
    [switch]$Local,
    [string]$BaseUrl = ''
)

# Production build laeuft jetzt mit leerem baseurl, weil GitHub Pages den
# Site unter der Custom Domain https://surgewave.io serviert (site/CNAME).
# -Local laesst die Default-Konfiguration unveraendert (auch leer), das ist
# hier nur ein historischer No-Op damit Aufrufe wie `-Local -SkipDotnet -Serve`
# weiter funktionieren ohne Argument-Parsing-Brueche.
if ($Local) { $BaseUrl = '' }

$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if (-not $SkipDotnet) {
    Write-Host '==> Building Surgewave solution (Release) for DocFX API metadata'
    & dotnet build Kuestenlogik.Surgewave.slnx -c Release --nologo -v minimal
    if ($LASTEXITCODE -ne 0) { throw 'dotnet build failed' }
}

Write-Host '==> Running DocFX'
& docfx docs/docfx.json
if ($LASTEXITCODE -ne 0) { throw 'docfx build failed' }

Write-Host '==> Building Jekyll site'
Push-Location site
try {
    & bundle install --quiet
    if ($LASTEXITCODE -ne 0) { throw 'bundle install failed' }
    & bundle exec jekyll build "--baseurl=$BaseUrl"
    if ($LASTEXITCODE -ne 0) { throw 'jekyll build failed' }
} finally {
    Pop-Location
}

Write-Host '==> Combining Jekyll + DocFX outputs into _combined/'
$Combined = Join-Path $Root '_combined'
if (Test-Path $Combined) { Remove-Item -Recurse -Force $Combined }
New-Item -ItemType Directory -Path $Combined | Out-Null
Copy-Item -Recurse (Join-Path $Root 'site\_site\*') $Combined
$CombinedDocs = Join-Path $Combined 'docs'
New-Item -ItemType Directory -Path $CombinedDocs | Out-Null
Copy-Item -Recurse (Join-Path $Root 'artifacts\docs\*') $CombinedDocs

Write-Host '==> Generating Pagefind search index over the combined output'
# Force npx.cmd: the npx.ps1 shim that ships with npm 11 + Node 24 mangles
# the first positional argument under PowerShell (drops the leading two
# characters, so "pagefind" becomes "px" and npx tries to download the wrong
# package). The .cmd shim parses arguments correctly.
$NpxCmd = (Get-Command npx.cmd -ErrorAction SilentlyContinue)?.Source
if (-not $NpxCmd) { $NpxCmd = 'npx.cmd' }
& $NpxCmd -y pagefind --site $Combined --output-path (Join-Path $Combined 'pagefind')
if ($LASTEXITCODE -ne 0) { throw 'pagefind failed' }

Write-Host ''
Write-Host "==> Done. Combined site is at $Combined"
Write-Host "    Open $Combined\index.html or run with -Serve to host it."

if ($Serve) {
    Write-Host ''
    Write-Host '==> Hosting _combined/ on http://localhost:4000 (Ctrl+C to stop)'
    # Jekyll serve would re-run only on site/ changes; the combined tree includes
    # the DocFX docs which Jekyll never sees. Use python's stdlib server instead
    # so what you see is exactly what GitHub Pages will serve.
    Push-Location $Combined
    try {
        & python -m http.server 4000
    } finally {
        Pop-Location
    }
}
