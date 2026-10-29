#requires -Version 7.5
<#
.SYNOPSIS
    Internal to scripts/stack.ps1: the stacks' life log, artifacts/stack.log.

.DESCRIPTION
    Dot-sourced by scripts/stack.ps1 and by the console a stack runs each part in (scripts/stack/part.ps1). Each
    writes one line per event: which verb was called with which parameters, which console and process it started or
    ended, and how a part ended, its exit code and how long it ran. One log for every stack of a checkout; a line
    names its stack's offset. A part that ends while nobody stopped it, and one that vanished without a word, leave
    a line of their own, so an unexplained end can be read back afterwards.

    Lines carry "pid=<n>" for a process the stack started; the script that finds such a process gone without a
    logged end says so once (Write-StackVanished).

    Writers are serialised by a named mutex, a handshake and not a pause, so the console and the script never
    interleave half lines. The convention is Terracle's (its scripts/stack/log.ps1); the code is this repository's
    own copy, as Modelzero keeps one, until a shared module is decided on.
#>

# SURGEWAVE_STACK_LOG names another file: the tests of the script must not write into a checkout's log.
$script:StackLogPath = if ($env:SURGEWAVE_STACK_LOG) { $env:SURGEWAVE_STACK_LOG } else { Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) 'artifacts' 'stack.log' }

function Write-StackLog {
    param(
        [Parameter(Mandatory, Position = 0)] [string] $Message,
        [string] $Source = 'script'
    )
    $line = '{0}  {1,-10} {2}{3}' -f (Get-Date).ToString('yyyy-MM-dd HH:mm:ss.fff'), $Source, $Message, [Environment]::NewLine
    $mutex = [System.Threading.Mutex]::new($false, 'Local\SurgewaveStackLog')
    try {
        try { [void]$mutex.WaitOne() } catch [System.Threading.AbandonedMutexException] { }
        try {
            $dir = Split-Path -Parent $script:StackLogPath
            if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Force $dir | Out-Null }
            # Kept small: past a megabyte the log moves aside once (stack.log.1) and starts anew.
            if ((Test-Path $script:StackLogPath) -and (Get-Item $script:StackLogPath).Length -gt 1MB) {
                Move-Item -Force $script:StackLogPath "$script:StackLogPath.1"
            }
            [System.IO.File]::AppendAllText($script:StackLogPath, $line)
        } finally { $mutex.ReleaseMutex() }
    } catch {
        # The log is a witness, never a reason for a start or a stop to fail.
    } finally { $mutex.Dispose() }
}

<# "stack.ps1 start -Offset 100 -Brokers 3": the call as it was typed, switches and values. #>
function Format-StackCall {
    param([string] $Script, [System.Collections.IDictionary] $Bound)
    $parts = @($Script)
    foreach ($key in $Bound.Keys) {
        $value = $Bound[$key]
        if ($value -is [System.Management.Automation.SwitchParameter]) {
            if ($value.IsPresent) { $parts += "-$key" } else { $parts += "-${key}:`$false" }
        } elseif ($value -is [array]) {
            $parts += "-$key $($value -join ', ')"
        } else {
            $parts += "-$key $value"
        }
    }
    $parts -join ' '
}

<#
    Every process this log saw start ("pid=<n> started") and never saw end (no "ended", no "stopping", no earlier
    "vanished") and that no longer runs: its console was closed, or it was ended from outside without its console
    noticing. Said once per process.
#>
function Write-StackVanished {
    param([string] $Source = 'script')
    if (-not (Test-Path $script:StackLogPath)) { return }
    $open = [ordered]@{}
    foreach ($line in [System.IO.File]::ReadAllLines($script:StackLogPath)) {
        if ($line -match 'pid=(\d+) started') { $open[$Matches[1]] = $line; continue }
        if ($line -match 'pid=(\d+) (ended|vanished)' -or $line -match 'stopping \S+ pid=(\d+)') { $open.Remove($Matches[1]) }
    }
    foreach ($id in @($open.Keys)) {
        $alive = Get-Process -Id ([int]$id) -ErrorAction SilentlyContinue
        if ($alive -and $alive.ProcessName -like 'surgewave-*') { continue }
        $what = if ($open[$id] -match '^\S+ \S+\s+\S+\s+(.*?) pid=') { $Matches[1] } else { 'a process' }
        $since = $open[$id].Substring(0, 19)
        Write-StackLog -Source $Source "$what pid=$id vanished: started $since, gone without a logged end; its console was closed or it was ended from outside"
    }
}

<# "3h05m12s": how long a part ran, whole days included. #>
function Format-StackDuration {
    param([TimeSpan] $Span)
    '{0}h{1:00}m{2:00}s' -f [math]::Floor($Span.TotalHours), $Span.Minutes, $Span.Seconds
}

<# What an exit code says about how a part ended. #>
function Get-StackExitHint {
    param([int] $Code)
    switch ($Code) {
        0 { 'a clean end: Ctrl+C from stack stop when its "ended in order" line follows, or the host stopped itself' }
        -1 { 'terminated from outside: stack stop, Task Manager or Stop-Process' }
        -1073741510 { 'Ctrl+C or its console closed (STATUS_CONTROL_C_EXIT)' }
        default { 'failed; its log (stack log -Part <part>) shows why' }
    }
}
