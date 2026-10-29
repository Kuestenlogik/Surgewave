#requires -Version 7.5
# The blank line below keeps the help apart: a comment directly above <# joins the help block, and Get-Help then
# finds no help.

<#
.SYNOPSIS
    Internal to scripts/stack.ps1: whether the build output of the stack's parts is older than the checkout.

.DESCRIPTION
    Dot-sourced by scripts/stack.ps1. A build that is older than the code it should hold starts silently and shows
    yesterday's broker as if it were today's, so start and restart build when the build is stale. This is this
    repository's own copy of Modelzero's scripts/stack/staleness.ps1, for two output folders (the broker's and the
    Control UI's) instead of one.

    The sources are the tracked and untracked (not ignored) files under src/, the Directory.Build.* files,
    Directory.Packages.props, NuGet.config, global.json and the .slnx beside the solution. Git lists them
    (git ls-files -co --exclude-standard), so build output, obj/ and other ignored files never count.

    A build the stack makes leaves a stamp (Write-StackBuildStamp): when it began, HEAD's commit id and a hash of the
    source list as they were then, and the last write of the build output it left. The stamp answers what times alone
    cannot: an incremental build with nothing to compile leaves the DLLs as they were, a file edited while the build
    ran is older than the DLL the build wrote after it, a switch to an older commit or a deleted file makes no file
    newer.

    The build output is Surgewave's own assemblies (Kuestenlogik.Surgewave.*.dll and the hosts' surgewave-*.dll) in
    the output folders given; the newest of them stands for the build.
#>

<#
    Runs git on the checkout and nowhere else: GIT_DIR, GIT_WORK_TREE and GIT_INDEX_FILE, set by a hook or a git alias
    that runs this script, would point it at another repository. They are cleared for the call and put back after it.
#>
function Invoke-StackGit([string] $Repo, [string[]] $Arguments) {
    $saved = @{}
    foreach ($name in 'GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE') {
        $value = [Environment]::GetEnvironmentVariable($name)
        if ($null -eq $value) { continue }
        $saved[$name] = $value
        # Remove-Item, not SetEnvironmentVariable($name, $null): PowerShell passes $null to a string parameter as '',
        # and an empty GIT_DIR is a repository named ''.
        Remove-Item -LiteralPath "Env:$name"
    }
    try {
        $output = & git -C $Repo @Arguments 2>$null
        if ($LASTEXITCODE -ne 0) { throw "git $($Arguments -join ' ') failed in $Repo (exit code $LASTEXITCODE)." }
        "$output"
    } finally {
        foreach ($name in $saved.Keys) { Set-Item -LiteralPath "Env:$name" -Value $saved[$name] }
    }
}

<#
.SYNOPSIS
    The checkout as a build sees it: HEAD's commit id and time, the source files with their last writes, and a hash of
    their list.
#>
function Get-StackSources([Parameter(Mandatory)] [string] $Repo) {
    $head = (Invoke-StackGit $Repo @('log', '-1', '--format=%H %ct', 'HEAD')).Trim() -split ' '
    # NUL-separated, so no path is quoted or split.
    $listed = Invoke-StackGit $Repo @('-c', 'core.quotepath=off', 'ls-files', '-co', '--exclude-standard', '-z', '--',
        ':(glob)src/**', ':(glob)Directory.Build.*', 'Directory.Packages.props', ':(icase)NuGet.config', 'global.json', ':(glob)*.slnx')
    # A tracked file deleted in the working tree is still listed; it is left out, so its deletion changes the hash.
    $files = foreach ($relative in ("$listed" -split "`0" | Where-Object { $_ } | Sort-Object -Unique -CaseSensitive)) {
        $path = Join-Path $Repo $relative
        if ([IO.File]::Exists($path)) { [pscustomobject]@{ Path = $relative; Written = [IO.File]::GetLastWriteTimeUtc($path) } }
    }
    $files = @($files)
    $hash = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes(($files.Path -join "`n"))))
    [pscustomobject]@{
        Head     = $head[0]
        HeadTime = [DateTimeOffset]::FromUnixTimeSeconds([long]$head[1]).UtcDateTime
        Files    = $files
        Hash     = $hash
    }
}

# The newest of Surgewave's own DLLs in the output folders: a build that compiled only a library rewrites that one
# alone. Every folder must hold a build; one without is no build at all.
function Get-StackBuildOutput([Parameter(Mandatory)] [string[]] $Output) {
    $dlls = foreach ($folder in $Output) {
        if (-not (Test-Path -LiteralPath $folder -PathType Container)) { return $null }
        $own = @(Get-ChildItem -LiteralPath $folder -Filter '*.dll' -File | Where-Object { $_.Name -like 'Kuestenlogik.Surgewave.*.dll' -or $_.Name -like 'surgewave-*.dll' })
        if ($own.Count -eq 0) { return $null }
        $own
    }
    $dlls | Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1
}

<#
.SYNOPSIS
    Writes the stamp of a build that succeeded: $Sources as Get-StackSources read them before the build, $Began when it
    began, and the newest DLL the build left.
#>
function Write-StackBuildStamp {
    param(
        [Parameter(Mandatory)] [string] $Stamp,
        [Parameter(Mandatory)] [datetime] $Began,
        [Parameter(Mandatory)] $Sources,
        [Parameter(Mandatory)] [string[]] $Output
    )
    $dll = Get-StackBuildOutput $Output
    if (-not $dll) { throw "The build left none of Surgewave's DLLs in $($Output -join ' or ')." }
    New-Item -ItemType Directory -Force (Split-Path -Parent $Stamp) | Out-Null
    [ordered]@{
        began         = $Began.ToUniversalTime().Ticks
        head          = $Sources.Head
        sources       = $Sources.Hash
        output        = $dll.FullName
        outputWritten = $dll.LastWriteTimeUtc.Ticks
    } | ConvertTo-Json | Set-Content -LiteralPath $Stamp -Encoding utf8
}

<#
.SYNOPSIS
    Says why the build is stale, or nothing when it is not.

.DESCRIPTION
    While the newest DLL is the one the stamp records, the build is the stack's: it counts from when it began, and is
    stale when HEAD is another commit than it was then, when a source file was added or deleted, or when a source
    file was written after it began. Otherwise (no stamp, or a build outside the stack rewrote the output since) it
    counts from the newest DLL's last write, and is stale when HEAD's commit time or any source file is newer.

.PARAMETER Repo
    The checkout's root.

.PARAMETER Output
    The build output folders of the stack's parts.

.PARAMETER Stamp
    The stamp of the stack's last successful build; it need not exist.
#>
function Get-StackBuildStaleness {
    param(
        [Parameter(Mandatory)] [string] $Repo,
        [Parameter(Mandatory)] [string[]] $Output,
        [string] $Stamp
    )
    $dll = Get-StackBuildOutput $Output
    if (-not $dll) { return "there is no build output in $($Output -join ' and ')" }
    $at = { param($t) $t.ToLocalTime().ToString('yyyy-MM-dd HH:mm:ss') }
    $sources = Get-StackSources $Repo

    $stamped = $null
    if ($Stamp -and (Test-Path -LiteralPath $Stamp -PathType Leaf)) {
        try { $stamped = Get-Content -LiteralPath $Stamp -Raw | ConvertFrom-Json -AsHashtable } catch { $stamped = $null }
    }
    if ($stamped -and $stamped.output -eq $dll.FullName -and [long]$stamped.outputWritten -eq $dll.LastWriteTimeUtc.Ticks) {
        $builtAt = [datetime]::new([long]$stamped.began, [DateTimeKind]::Utc)
        if ($stamped.head -ne $sources.Head) { return "HEAD is $($sources.Head), the build was made at $($stamped.head)" }
        if ($stamped.sources -ne $sources.Hash) { return 'a source file was added or deleted since the build' }
    } else {
        $builtAt = $dll.LastWriteTimeUtc
        if ($sources.HeadTime -gt $builtAt) { return "HEAD's commit ($(& $at $sources.HeadTime)) is newer than the build ($(& $at $builtAt))" }
    }
    $newest = $sources.Files | Sort-Object Written -Descending | Select-Object -First 1
    if ($newest -and $newest.Written -gt $builtAt) { return "$($newest.Path) ($(& $at $newest.Written)) is newer than the build ($(& $at $builtAt))" }
    $null
}
