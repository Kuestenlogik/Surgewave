#!/usr/bin/env pwsh
<#
.SYNOPSIS
    End-to-end capture pipeline — startet Broker + Control, seedet Demo-Data,
    macht Screenshots + Video, räumt auf. Reproduzierbar, kein manueller Setup.

.DESCRIPTION
    Workflow:
      1. dotnet build (sicherstellen alles kompiliert)
      2. Surgewave-Broker im Background starten (Port 9092 + gRPC 5095)
      3. Auf Broker-Health warten (HTTP-Probe / TCP-Connect)
      4. Surgewave.Control im Background starten (Port 5050)
      5. Auf Control-Health warten
      6. (Optional) Demo-Data seeden via surgewave-CLI
      7. npm install + Playwright browsers installieren falls noch nicht
      8. capture-screenshots.js für dark + light themes
      9. record-demo.js für dark + light themes
     10. Cleanup: Broker + Control beenden

.PARAMETER SkipBuild
    Skip the dotnet build step (faster wenn nur captures neu).

.PARAMETER SkipSeed
    Skip the demo-data seeding step.

.PARAMETER Theme
    Theme to capture: dark, light, or both (default).

.EXAMPLE
    ./scripts/capture-pipeline.ps1
    Volle Pipeline: build + start broker/control + seed + capture + cleanup.

.EXAMPLE
    ./scripts/capture-pipeline.ps1 -SkipBuild -Theme dark
    Schnelle Iteration: build überspringen, nur dark theme.
#>
[CmdletBinding()]
param(
    [switch]$SkipBuild,
    [switch]$SkipSeed,
    [switch]$SkipPlugins,
    [switch]$AllConnectors,
    [string]$ConnectorsPath = "..\Surgewave.Connectors\artifacts\pluginPackage",
    [ValidateSet('dark', 'light', 'both')]
    [string]$Theme = 'both',
    [int]$ControlPort = 5050,
    [int]$BrokerKafkaPort = 9092,
    [int]$BrokerAdminPort = 9093,
    [int]$BrokerGrpcPort = 5095,
    [int]$WaitTimeout = 60
)

# Default-Auswahl: Connectoren ohne externe Service-Abhaengigkeit, die im
# leeren Broker schon plausibel aussehen (Marketplace-Liste, Connectors-Page).
# Mit -AllConnectors werden alle 118 .swpkg installiert.
$DemoConnectorNames = @(
    'Kuestenlogik.Surgewave.Connector.Akka',
    'Kuestenlogik.Surgewave.Connector.Csv',
    'Kuestenlogik.Surgewave.Connector.FileStream',
    'Kuestenlogik.Surgewave.Connector.Grok',
    'Kuestenlogik.Surgewave.Connector.Http',
    'Kuestenlogik.Surgewave.Connector.HttpServer',
    'Kuestenlogik.Surgewave.Connector.Mirror',
    'Kuestenlogik.Surgewave.Connector.Mqtt',
    'Kuestenlogik.Surgewave.Connector.Stdio',
    'Kuestenlogik.Surgewave.Connector.Tcp'
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host "=== Surgewave Capture Pipeline ===" -ForegroundColor Cyan
Write-Host "  Repo: $repoRoot" -ForegroundColor Gray
Write-Host "  Theme: $Theme" -ForegroundColor Gray
Write-Host ""

# ── Helper: HTTP-Health-Probe ───────────────────────────────────────────────
# Accepts any 2xx/3xx/4xx response (including 404) as "service-is-up" — the
# socket-bind alone is enough; route-existence isn't required for "alive".
function Wait-ForHealth([string]$url, [string]$service, [int]$timeoutSec) {
    Write-Host "  Waiting for $service @ $url …" -NoNewline
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-WebRequest -Uri $url -TimeoutSec 2 -UseBasicParsing -SkipCertificateCheck -ErrorAction Stop
            if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) {
                Write-Host " ready (HTTP $($r.StatusCode))" -ForegroundColor Green
                return $true
            }
        } catch [System.Net.Http.HttpRequestException] {
            # 4xx auch als "alive" werten — socket antwortet
            if ($_.Exception.Message -match '4[0-9]{2}') {
                Write-Host " ready (server responding)" -ForegroundColor Green
                return $true
            }
        } catch {}
        Write-Host "." -NoNewline
        Start-Sleep -Milliseconds 500
    }
    Write-Host " TIMEOUT" -ForegroundColor Red
    return $false
}

function Wait-ForTcp([string]$hostName, [int]$port, [string]$service, [int]$timeoutSec) {
    Write-Host "  Waiting for $service @ ${hostName}:${port} …" -NoNewline
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $client = New-Object System.Net.Sockets.TcpClient
            $task = $client.ConnectAsync($hostName, $port)
            if ($task.Wait(500)) {
                $client.Close()
                Write-Host " ready" -ForegroundColor Green
                return $true
            }
            $client.Close()
        } catch {}
        Write-Host "." -NoNewline
        Start-Sleep -Milliseconds 500
    }
    Write-Host " TIMEOUT" -ForegroundColor Red
    return $false
}

$brokerProc = $null
$controlProc = $null

try {
    # ── Step 1: Build ───────────────────────────────────────────────────────
    if (-not $SkipBuild) {
        Write-Host "[1/10] Building solution…" -ForegroundColor Yellow
        & dotnet build Kuestenlogik.Surgewave.slnx --configuration Release --verbosity minimal
        if ($LASTEXITCODE -ne 0) { throw "dotnet build failed" }
        Write-Host ""
    }

    # ── Step 2: Install demo connector plugins ──────────────────────────────
    # Wir deployen die .swpkg's nach .tmp/broker-plugins/ (ausserhalb des src-Trees) und
    # ueberreichen den absoluten Pfad als --Surgewave:Connect:PluginsDirectory beim Broker-
    # Start. Damit es keine Casing-Kollision mit dem existing Plugins/ Source-Dir gibt:
    # NTFS ist case-insensitive — vorherige Versionen haben versehentlich Sources gelöscht.
    if (-not $SkipPlugins) {
        Write-Host "[2/10] Installing demo connector plugins…" -ForegroundColor Yellow
        $brokerPluginsDir = Join-Path $repoRoot ".tmp/broker-plugins"
        New-Item -ItemType Directory -Force -Path $brokerPluginsDir | Out-Null

        $absConnectorsPath = if ([System.IO.Path]::IsPathRooted($ConnectorsPath)) { $ConnectorsPath } else { Join-Path $repoRoot $ConnectorsPath }
        if (-not (Test-Path $absConnectorsPath)) {
            Write-Host "  Connectors-pluginPackage dir not found: $absConnectorsPath" -ForegroundColor DarkYellow
            Write-Host "  Run 'cd ..\Surgewave.Connectors && pwsh scripts/build-and-pack.ps1 -SkipTests' to build .swpkg's." -ForegroundColor DarkYellow
            Write-Host "  Skipping plugin install — screenshots will show empty Connectors/Marketplace pages." -ForegroundColor DarkYellow
        }
        else {
            $allPkgs = Get-ChildItem -Path $absConnectorsPath -Filter "*.swpkg" -File
            if ($AllConnectors) {
                $toInstall = $allPkgs
                Write-Host "  Installing ALL $($toInstall.Count) connector plugins (-AllConnectors)" -ForegroundColor Gray
            }
            else {
                $toInstall = $allPkgs | Where-Object {
                    $name = $_.BaseName -replace '-\d+(\.\d+)+(-.*)?$',''
                    $DemoConnectorNames -contains $name
                }
                Write-Host "  Installing $($toInstall.Count) of $($allPkgs.Count) demo connectors (use -AllConnectors for all)" -ForegroundColor Gray
            }

            $installed = 0
            $failed = 0
            foreach ($pkg in $toInstall) {
                # surgewave-CLI lokal via 'dotnet run --project ...' starten — vermeidet, dass
                # eine global installierte (alte) Version verwendet wird.
                & dotnet run --project src/Kuestenlogik.Surgewave.Cli --no-build -c Release -- plugins install $pkg.FullName --directory $brokerPluginsDir --force 2>&1 | Out-Null
                if ($LASTEXITCODE -eq 0) {
                    $installed++
                    Write-Host "    $($pkg.BaseName)" -ForegroundColor Gray
                } else {
                    $failed++
                    Write-Host "    $($pkg.BaseName) — FAILED" -ForegroundColor Red
                }
            }
            Write-Host "  $installed installed, $failed failed" -ForegroundColor Green
        }
        Write-Host ""
    }

    # ── Step 3: Start Broker ────────────────────────────────────────────────
    Write-Host "[3/10] Starting Surgewave-Broker…" -ForegroundColor Yellow
    $brokerLog = Join-Path $repoRoot ".tmp/broker.log"
    New-Item -ItemType Directory -Force -Path (Split-Path $brokerLog) | Out-Null
    # Override PluginsDirectory damit der Broker im .tmp/broker-plugins-Pfad nach
    # installierten Connectoren sucht (statt im Standard <CWD>/plugins).
    $brokerPluginsArg = Join-Path $repoRoot ".tmp/broker-plugins"
    $brokerProc = Start-Process -FilePath dotnet `
        -ArgumentList @('run', '--project', 'src/Kuestenlogik.Surgewave.Broker', '--no-build', '-c', 'Release', '--', "--Surgewave:Connect:PluginsDirectory=$brokerPluginsArg") `
        -WorkingDirectory $repoRoot `
        -PassThru `
        -RedirectStandardOutput $brokerLog `
        -RedirectStandardError "$brokerLog.err" `
        -NoNewWindow
    Write-Host "  Broker PID: $($brokerProc.Id)  (log: $brokerLog)" -ForegroundColor Gray
    # Broker exposes gRPC + admin REST on HTTPS :9093 (Kestrel-bound, "https://*:9093" in appsettings.json).
    # 404 is fine as health-signal — only the socket-bind matters.
    if (-not (Wait-ForHealth "https://127.0.0.1:$BrokerAdminPort/" 'Broker (admin port)' $WaitTimeout)) {
        throw "Broker did not start within $WaitTimeout sec"
    }
    Write-Host ""

    # ── Step 4: Start Control ───────────────────────────────────────────────
    Write-Host "[4/10] Starting Surgewave.Control on :$ControlPort…" -ForegroundColor Yellow
    $controlLog = Join-Path $repoRoot ".tmp/control.log"
    # Don't override ASPNETCORE_URLS — Control's appsettings.json has Kestrel:Endpoints:Http:Url = http://*:5050
    $controlProc = Start-Process -FilePath dotnet `
        -ArgumentList @('run', '--project', 'src/Kuestenlogik.Surgewave.Control', '--no-build', '-c', 'Release') `
        -WorkingDirectory $repoRoot `
        -PassThru `
        -RedirectStandardOutput $controlLog `
        -RedirectStandardError "$controlLog.err" `
        -NoNewWindow
    Write-Host "  Control PID: $($controlProc.Id)  (log: $controlLog)" -ForegroundColor Gray
    # TCP-Probe statt HTTP — robuster wenn DI-Issues HTTP-Render brechen
    if (-not (Wait-ForTcp '127.0.0.1' $ControlPort 'Control' $WaitTimeout)) {
        throw "Control did not start within $WaitTimeout sec"
    }
    Write-Host ""

    # ── Step 5: Seed Demo-Data ──────────────────────────────────────────────
    if (-not $SkipSeed) {
        Write-Host "[5/10] Seeding demo data…" -ForegroundColor Yellow
        $seedScript = Join-Path $repoRoot 'scripts/seed-demo-data.ps1'
        if (Test-Path $seedScript) {
            & $seedScript -BrokerPort $BrokerKafkaPort
        } else {
            Write-Host "  scripts/seed-demo-data.ps1 not found — skipping (screenshots may show empty states)" -ForegroundColor DarkYellow
        }
        Write-Host ""
    }

    # ── Step 6: Install Playwright if needed ────────────────────────────────
    Write-Host "[6/10] Verify Playwright is installed…" -ForegroundColor Yellow
    if (-not (Test-Path "$repoRoot/node_modules/@playwright/test")) {
        Write-Host "  Running npm install…" -ForegroundColor Gray
        & npm install
        if ($LASTEXITCODE -ne 0) { throw "npm install failed" }
    }
    if (-not (Test-Path "$env:USERPROFILE/AppData/Local/ms-playwright")) {
        Write-Host "  Installing Playwright Chromium…" -ForegroundColor Gray
        & npx playwright install chromium
    }
    Write-Host ""

    # ── Step 7 + 8: Captures ────────────────────────────────────────────────
    $themes = if ($Theme -eq 'both') { @('dark', 'light') } else { @($Theme) }

    foreach ($t in $themes) {
        Write-Host "[7/10] Capturing screenshots — theme=$t" -ForegroundColor Yellow
        $env:THEME = $t
        $env:CONTROL_URL = "http://localhost:$ControlPort"
        & node scripts/capture-screenshots.js
        if ($LASTEXITCODE -ne 0) { Write-Host "  WARN: capture-screenshots returned $LASTEXITCODE" -ForegroundColor DarkYellow }
        Write-Host ""

        Write-Host "[8/10] Recording demo video — theme=$t" -ForegroundColor Yellow
        & node scripts/record-demo.js
        if ($LASTEXITCODE -ne 0) { Write-Host "  WARN: record-demo returned $LASTEXITCODE" -ForegroundColor DarkYellow }
        Write-Host ""
    }

    Write-Host "[9/10] Captures complete — see site/assets/{images/screenshots,videos}/" -ForegroundColor Green

} catch {
    Write-Host ""
    Write-Host "PIPELINE FAILED: $_" -ForegroundColor Red
    Write-Host "Broker-Log (tail):" -ForegroundColor DarkYellow
    if (Test-Path "$repoRoot/.tmp/broker.log") { Get-Content "$repoRoot/.tmp/broker.log" -Tail 30 }
    Write-Host "Control-Log (tail):" -ForegroundColor DarkYellow
    if (Test-Path "$repoRoot/.tmp/control.log") { Get-Content "$repoRoot/.tmp/control.log" -Tail 30 }
    $exitCode = 1
} finally {
    # ── Step 10: Cleanup ────────────────────────────────────────────────────
    Write-Host ""
    Write-Host "[10/10] Cleanup…" -ForegroundColor Yellow
    foreach ($proc in @($controlProc, $brokerProc)) {
        if ($proc -and -not $proc.HasExited) {
            Write-Host "  Stopping PID $($proc.Id)…" -NoNewline
            try {
                # Try graceful first
                $null = $proc.CloseMainWindow()
                if (-not $proc.WaitForExit(3000)) {
                    $proc.Kill($true)
                }
                Write-Host " stopped" -ForegroundColor Green
            } catch {
                Write-Host " kill failed: $_" -ForegroundColor Red
            }
        }
    }
}

if ($exitCode) { exit $exitCode }
Write-Host ""
Write-Host "Done. Don't forget to rebuild the site to pick up new images:" -ForegroundColor Gray
Write-Host "  ./scripts/build-site.ps1  (or local Jekyll build)" -ForegroundColor Gray
