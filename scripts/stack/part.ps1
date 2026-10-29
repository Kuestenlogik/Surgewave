#requires -Version 7.5
<#
.SYNOPSIS
    Internal to scripts/stack.ps1: one part of a stack (a broker or the Control UI) in a console of its own.

.DESCRIPTION
    scripts/stack.ps1 starts every part through this script, in a hidden console. The part's output goes, line by
    line as it comes, into its log file (artifacts/stacks/<offset>/logs/<part>.log). The console's command line names
    this script, the stack's offset and the part, which is how stack stop, restart and status recognise it; the
    part's settings come from a file beside the stack's state.

    The console takes Ctrl+C for itself and the part before it starts it (scripts/stack/console.ps1): a stack started
    from a process that ignores Ctrl+C would otherwise hand that on, and stop could not end the part in order.

    It records the part (-RecordFile: process id, program, start time), so stack stop and status still find a part
    whose console was ended from outside while it ran on, and never take a later process that got the same id for it.

    The readiness handshake: when the part says its host has started (Kestrel listens by then, and a broker has
    started its Kafka listener before it), the console sets the named event -ReadyEvent that stack.ps1 waits on,
    together with this console's exit. No pause and no polling.

    The console also keeps the part's life log (scripts/stack/log.ps1): the process id it started and, when the
    process ends (stopped, crashed), its exit code and how long it ran.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [int] $Offset,
    [Parameter(Mandatory)] [string] $Part,
    [Parameter(Mandatory)] [string] $Exe,
    [Parameter(Mandatory)] [string] $EnvFile,
    [Parameter(Mandatory)] [string] $LogFile,
    [Parameter(Mandatory)] [string] $RecordFile,
    [string] $ReadyEvent,
    [string] $Label = $Part
)

$ErrorActionPreference = 'Stop'

# The life log first: it takes its file from SURGEWAVE_STACK_LOG, which the clearing below removes.
. (Join-Path $PSScriptRoot 'log.ps1')
. (Join-Path $PSScriptRoot 'console.ps1')

# The part's settings are the stack's and nothing else: a Surgewave__*, Kestrel__*, SURGEWAVE_* or a listening address
# in the caller's environment would otherwise reach the part unseen and, say, point it at another data directory or
# another port. Configuration keys are matched without regard to case, as the host reads them.
$foreign = 'Surgewave__*', 'SURGEWAVE_*', 'Kestrel__*', 'Broker__*', 'Gateway__*'
foreach ($name in @([Environment]::GetEnvironmentVariables().Keys)) {
    if (@($foreign | Where-Object { $name -like $_ }).Count -gt 0 -or $name -in 'ASPNETCORE_URLS', 'ASPNETCORE_HTTP_PORTS', 'ASPNETCORE_HTTPS_PORTS', 'DOTNET_URLS') {
        [Environment]::SetEnvironmentVariable($name, $null)
    }
}
foreach ($pair in (Get-Content -Path $EnvFile -Raw | ConvertFrom-Json -AsHashtable).GetEnumerator()) {
    [Environment]::SetEnvironmentVariable($pair.Key, [string]$pair.Value)
}
[Console]::OutputEncoding = [Text.Encoding]::UTF8
Enable-StackCtrlC

New-Item -ItemType Directory -Force (Split-Path -Parent $LogFile) | Out-Null
$log = [IO.StreamWriter]::new($LogFile, $true, [Text.UTF8Encoding]::new($false))
$log.AutoFlush = $true
$colour = [regex]::new('\x1b\[[0-9;?]*[A-Za-z]')

# The generic host's own word that it has started: Kestrel has bound its endpoints by then. The broker starts its
# Kafka listener before it runs the host, so the line comes after that too.
$startedLine = [regex]::new('Application started\.')
$ready = $null
if ($ReadyEvent) {
    try { $ready = [System.Threading.EventWaitHandle]::OpenExisting($ReadyEvent) } catch { $ready = $null }
}

$psi = [Diagnostics.ProcessStartInfo]::new($Exe)
$psi.WorkingDirectory = Split-Path -Parent $Exe
$psi.UseShellExecute = $false
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError = $true
$psi.StandardOutputEncoding = [Text.UTF8Encoding]::new($false)
$psi.StandardErrorEncoding = [Text.UTF8Encoding]::new($false)

$streams = @($null, $null)
$pending = @($null, $null)
$open = @($true, $true)

# Both streams line by line as they come, into the file. Each read is awaited: the next line or the stream's end is
# the signal, never a pause. Returns once the part closed both.
function Copy-PartOutput {
    while ($open[0] -or $open[1]) {
        $active = @(0, 1 | Where-Object { $open[$_] })
        foreach ($i in $active) { if ($null -eq $pending[$i]) { $pending[$i] = $streams[$i].ReadLineAsync() } }
        $index = [Threading.Tasks.Task]::WaitAny([Threading.Tasks.Task[]]@($active | ForEach-Object { $pending[$_] }))
        $i = $active[$index]
        $line = $pending[$i].GetAwaiter().GetResult()
        $pending[$i] = $null
        if ($null -eq $line) { $open[$i] = $false; continue }
        $log.WriteLine($colour.Replace($line, ''))
        if ($ready -and $startedLine.IsMatch($line)) { [void]$ready.Set() }
    }
}

$started = Get-Date
$process = $null
$log.WriteLine(('---- {0} started {1:yyyy-MM-dd HH:mm:ss} from {2}' -f $Label, $started, $Exe))
try {
    $process = [Diagnostics.Process]::Start($psi)
    $null = $process.Handle   # keeps the exit code readable after the part is gone
    $streams[0] = $process.StandardOutput
    $streams[1] = $process.StandardError
    Write-StackLog -Source 'console' ('{0} pid={1} started in console {2}' -f $Label, $process.Id, $PID)
    New-Item -ItemType Directory -Force (Split-Path -Parent $RecordFile) | Out-Null
    @{ pid = $process.Id; exe = $Exe; startedUtcTicks = $process.StartTime.ToUniversalTime().Ticks; console = $PID } |
        ConvertTo-Json | Set-Content -Path $RecordFile -Encoding utf8
    Copy-PartOutput
    $process.WaitForExit()
} catch {
    $log.WriteLine("---- $Label did not start: $($_.Exception.Message)")
    Write-StackLog -Source 'console' "$Label did not start in console ${PID}: $($_.Exception.Message)"
} finally {
    # Also after a Ctrl+C, which stops this script at its next step: the part got the same Ctrl+C, and what it
    # writes while it shuts down still goes to the file before its end is logged.
    if ($process) {
        try { Copy-PartOutput } catch { }
        $process.WaitForExit()
        $ran = Format-StackDuration ((Get-Date) - $started)
        Write-StackLog -Source 'console' ('{0} pid={1} ended with exit code {2} (0x{2:X8}) after {3}: {4}' -f $Label, $process.Id, $process.ExitCode, $ran, (Get-StackExitHint $process.ExitCode))
        $log.WriteLine(('---- {0} ended with exit code {1} after {2}' -f $Label, $process.ExitCode, $ran))
        Remove-Item -LiteralPath $RecordFile -ErrorAction SilentlyContinue
    }
    $log.Dispose()
    if ($ready) { $ready.Dispose() }
}
