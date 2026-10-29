#requires -Version 7.5
<#
.SYNOPSIS
    Internal to scripts/stack.ps1: Ctrl+C for the console a part runs in, taking it and sending it.

.DESCRIPTION
    Dot-sourced by scripts/stack.ps1 (which sends) and by a part's console (scripts/stack/part.ps1, which takes it). A
    stack stops its parts (the brokers and the Control UI) in order by Ctrl+C in each part's console, and the part's
    exit is the handshake.

    Windows passes "ignore Ctrl+C" on from a process to every process it starts: whoever runs stack.ps1 from a
    process that ignores it (a tool's or an agent's shell, a background job) hands it on through the console to the
    part. The console then receives the event, but no handler in it runs, the part never shuts down, and the stop
    would have to end it from outside. So the console takes Ctrl+C back before it starts the part
    (Enable-StackCtrlC), and the part always gets the Ctrl+C its console receives. This is this repository's own copy
    of the scripts/stack/console.ps1 of Terracle and Modelzero.
#>

<#
    Takes Ctrl+C for this process and every process it starts from now on: clears the "ignore Ctrl+C" this process
    may have been started with. Called by the console before it starts the part, which inherits the setting.
#>
function Enable-StackCtrlC {
    if (-not $IsWindows) { return }
    Add-Type -Namespace Surgewave -Name ConsoleCtrlC -MemberDefinition @'
[DllImport("kernel32.dll", SetLastError = true)] public static extern bool SetConsoleCtrlHandler(System.IntPtr handler, bool add);
'@
    if (-not [Surgewave.ConsoleCtrlC]::SetConsoleCtrlHandler([IntPtr]::Zero, $false)) {
        throw ('Ctrl+C cannot be taken back for this console (error {0})' -f [Runtime.InteropServices.Marshal]::GetLastWin32Error())
    }
}

<#
    Ctrl+C in a part's console, and the part's exit awaited. A console control event can only be raised from inside
    the console it is meant for, so this runs in a process of its own without a console (Stop-StackParts starts it,
    with this function's text): it attaches to the console the part is in, through the part itself so a part whose
    console ended is reached as well, ignores the event itself, raises it, and waits for the part to exit: the exit
    is the handshake. The bound only guards against a part that hangs in its shutdown; a broker hands its partitions
    over before it ends, which takes longer than a plain host's 30 s. Writes one line: "ended <exit code> after <ms>
    ms", "timeout" or "failed: <why>".
#>
function Send-StackCtrlC {
    param([int] $ConsolePid, [int] $PartPid, [int] $TimeoutSeconds = 90)
    $ErrorActionPreference = 'Stop'
    Add-Type -Namespace Surgewave -Name ConsoleControl -MemberDefinition @'
[DllImport("kernel32.dll", SetLastError = true)] public static extern bool FreeConsole();
[DllImport("kernel32.dll", SetLastError = true)] public static extern bool AttachConsole(uint processId);
[DllImport("kernel32.dll", SetLastError = true)] public static extern bool SetConsoleCtrlHandler(System.IntPtr handler, bool add);
[DllImport("kernel32.dll", SetLastError = true)] public static extern bool GenerateConsoleCtrlEvent(uint ctrlEvent, uint processGroupId);
'@
    try {
        $target = [System.Diagnostics.Process]::GetProcessById($PartPid)
        $null = $target.Handle   # keeps the exit code readable after the part is gone
    } catch {
        return 'failed: the part is not running'
    }
    $watch = [System.Diagnostics.Stopwatch]::StartNew()
    [void][Surgewave.ConsoleControl]::FreeConsole()
    if (-not [Surgewave.ConsoleControl]::AttachConsole([uint32]$PartPid) -and -not [Surgewave.ConsoleControl]::AttachConsole([uint32]$ConsolePid)) {
        return ('failed: cannot attach to the console of the part or of console {0} (error {1})' -f $ConsolePid, [Runtime.InteropServices.Marshal]::GetLastWin32Error())
    }
    try {
        # This process ignores the event; everything else in the console receives it.
        [void][Surgewave.ConsoleControl]::SetConsoleCtrlHandler([IntPtr]::Zero, $true)
        if (-not [Surgewave.ConsoleControl]::GenerateConsoleCtrlEvent(0, 0)) {
            return ('failed: Ctrl+C not raised (error {0})' -f [Runtime.InteropServices.Marshal]::GetLastWin32Error())
        }
        if ($target.WaitForExit($TimeoutSeconds * 1000)) { 'ended {0} after {1} ms' -f $target.ExitCode, $watch.ElapsedMilliseconds }
        else { 'timeout' }
    } finally {
        [void][Surgewave.ConsoleControl]::FreeConsole()
    }
}
