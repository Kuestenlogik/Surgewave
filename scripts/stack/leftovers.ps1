#requires -Version 7.5
<#
.SYNOPSIS
    Internal to scripts/stack.ps1: the processes a stack's parts leave behind after a stop.

.DESCRIPTION
    Dot-sourced by scripts/stack.ps1. A part's processes are the part and everything it started. Before a stop
    the stack takes their list (Get-StackProcessTree); after it, whatever of that list still runs is a leftover
    (Find-StackLeftovers), and the stop names it. A process is the one in the list only with the same id and the same
    start time, so a later process that got a freed id is never taken for it. Nothing here ends a process: a leftover
    is reported, not killed. This repository's own copy of the scripts/stack/leftovers.ps1 of Terracle and Modelzero.
#>

<#
    The processes under the given root processes, the roots included: each with its id, name, start time, parent and
    the root it belongs to. One snapshot of the system's process list; a process whose parent is in the tree is in it.
#>
function Get-StackProcessTree([int[]] $Roots) {
    if (-not $Roots -or $Roots.Count -eq 0) { return @() }
    $all = @(Get-CimInstance Win32_Process -Property ProcessId, ParentProcessId, Name, CreationDate)
    $byParent = @{}
    foreach ($p in $all) {
        $key = [int]$p.ParentProcessId
        if (-not $byParent.ContainsKey($key)) { $byParent[$key] = [System.Collections.Generic.List[object]]::new() }
        $byParent[$key].Add($p)
    }
    $tree = [System.Collections.Generic.List[object]]::new()
    foreach ($rootId in $Roots) {
        $root = $all | Where-Object { [int]$_.ProcessId -eq $rootId } | Select-Object -First 1
        if (-not $root) { continue }
        $queue = [System.Collections.Generic.Queue[object]]::new()
        $queue.Enqueue($root)
        while ($queue.Count -gt 0) {
            $p = $queue.Dequeue()
            $tree.Add([pscustomobject]@{ ProcessId = [int]$p.ProcessId; Name = $p.Name; Started = $p.CreationDate; ParentId = [int]$p.ParentProcessId; RootId = $rootId })
            if (-not $byParent.ContainsKey([int]$p.ProcessId)) { continue }
            foreach ($child in $byParent[[int]$p.ProcessId]) {
                # The host of a console a process opened ends with the console's last process: never left over by itself.
                if ($child.Name -eq 'conhost.exe') { continue }
                # A child starts after its parent; an older process under a reused id is not the parent's.
                if ($child.CreationDate -and $p.CreationDate -and $child.CreationDate -lt $p.CreationDate) { continue }
                $queue.Enqueue($child)
            }
        }
    }
    $tree
}

<# The processes of a tree taken by Get-StackProcessTree that still run: the same id and the same start time. #>
function Find-StackLeftovers([object[]] $Tree) {
    if (-not $Tree -or $Tree.Count -eq 0) { return @() }
    $running = @{}
    foreach ($p in @(Get-CimInstance Win32_Process -Property ProcessId, CreationDate)) { $running[[int]$p.ProcessId] = $p.CreationDate }
    @($Tree | Where-Object { $running.ContainsKey($_.ProcessId) -and $running[$_.ProcessId] -eq $_.Started })
}
