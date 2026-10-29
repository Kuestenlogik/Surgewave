#requires -Version 7.5
# The blank line below keeps the help apart: a comment directly above <# joins the help block, and Get-Help then
# finds no help (the script shows its help when called without a verb).

<#
.SYNOPSIS
    The local stack: start, stop, restart, status, log and build Surgewave brokers and the Control UI, one entry for
    all of it.

.DESCRIPTION
    A stack is one broker, or a cluster of several (-Brokers), and the Control UI talking to broker 1, each part
    started from a copy of this checkout's build output, in a hidden console of its own. Several stacks run side by
    side, told apart by their port offset (-Offset). SURGEWAVE_STACK_OFFSET, when set, is the offset of every verb
    that is not given one.

      stack.ps1 start   [-Offset n] [-Brokers n] [-Hidden] [-Build | -NoBuild] [-ReadyDeadline <seconds>]
      stack.ps1 stop    [-Offset n] [-Force]
      stack.ps1 restart [-Offset n] [-Build | -NoBuild] [-ReadyDeadline <seconds>]
      stack.ps1 status  [-Offset n]
      stack.ps1 log     [-Offset n] [-Part control|broker<n>] [-Tail n] [-Follow]
      stack.ps1 build

    The verbs, -Offset, artifacts/stacks/<offset>/ and the life log are the convention Terracle's scripts/stack.ps1
    set and Modelzero's copies; this is Surgewave's own copy of what brokers and the Control UI need, not a shared
    module.

    The ports. Every port comes from the parts' own appsettings.json, as the build output holds it, moved by the
    offset; a broker of a cluster is moved by 10 more for each broker before it. With today's configuration broker n
    of the stack at offset o takes
      Kafka            Surgewave:Port (9092)                + o + 10 (n - 1)
      gRPC and REST    Surgewave:GrpcPort (9093)            + o + 10 (n - 1)   https while Surgewave:GrpcUseTls is on
      replication      Surgewave:ReplicationPort (10092)    + o + 10 (n - 1)
      and every other Kestrel endpoint of its configuration (GrpcHttp3, 9094, TCP and UDP) moved the same way, and
      the PostgreSQL wire port (Surgewave:PostgreSql:Port, 5432) when the configuration switches it on;
    and the Control UI takes the port of its Kestrel:Endpoints:Http:Url (5050) + o. The Kafka bootstrap list is
    every broker's Surgewave:Host and Kafka port. No part opens a port of its own beside these: each runs with its
    content root pinned to its copy of the build (ASPNETCORE_CONTENTROOT), so it always finds its appsettings.json
    and never falls back to ASP.NET Core's default http://localhost:5000.

    stack.json, for people and for other scripts (Terracle's stack reads it to use a Surgewave stack):
      {
        "offset": 0, "brokerCount": 3, "startedAt": "<ISO 8601>",
        "host": "localhost",
        "bootstrap": "localhost:9092,localhost:9102,localhost:9112",
        "controlUrl": "http://localhost:5050",
        "brokers": [ { "name": "broker1", "id": 1, "kafka": 9092, "grpc": 9093, "replication": 10092,
                       "postgreSql": null, "rest": "https://localhost:9093",
                       "endpoints": { "Grpc": "https://*:9093", "GrpcHttp3": "https://*:9094" }, "udp": [ 9094 ] }, ... ],
        "control": { "name": "control", "port": 5050, "endpoint": "http://*:5050", "url": "http://localhost:5050" }
      }
    It is written when the parts start; whether they still run is status's answer, not the file's.

    Where a stack keeps what it has: artifacts/stacks/<offset>/ holds how it was started (stack.json, read by restart
    and status: the number of brokers, the ports and the bootstrap list), the settings of each part (env/<part>.json),
    the copies of the build it runs (bin/broker/, bin/control/), each part's data (data/broker<n>/, data/control/),
    each part's output (logs/<part>.log, the run before it logs/<part>.previous.log, a run that failed
    logs/<part>.failed-<when>.log) and the record of each running part (parts/<part>.json). artifacts/stack.log is
    the life log of every stack of the checkout: every call, every console and process started or ended, and how
    each part ended. SURGEWAVE_STACK_ROOT names another folder for artifacts/stacks and SURGEWAVE_STACK_LOG another
    life log.

    Five details are not obvious:

    start and restart build when the build is stale (scripts/stack/staleness.ps1). The sources are the tracked and
    untracked (not ignored) files under src/, the Directory.Build.* files, Directory.Packages.props, NuGet.config,
    global.json and the .slnx. A build the stack made leaves artifacts/stack-build.stamp: when it began, HEAD's commit
    id, a hash of the source list and the newest Surgewave DLL it left. While that DLL is unchanged, the build is
    stale when HEAD is another commit, a source file was added or deleted, or one was written after the build began;
    a build made outside the stack counts from its newest Surgewave DLL, and is stale when HEAD's commit or a source
    file is newer. -NoBuild starts what is there, and says so when it is stale. The build is the two hosts in Debug
    (dotnet build of Kuestenlogik.Surgewave.Broker.App and Kuestenlogik.Surgewave.Control), its output in
    artifacts/stack-build.log.

    Each part runs from a copy of its build output (artifacts/bin/Kuestenlogik.Surgewave.Broker.App/Debug/ and
    artifacts/bin/Kuestenlogik.Surgewave.Control/Debug/), made at every start and restart. A running part holds its
    program's files open, and a build into that folder then fails with MSB3021; with the copy, builds go on while
    stacks run, and a restart picks the new build up. The broker's copy carries plugins/, where the build stages the
    Kafka protocol plugin (CopyKafkaPluginForDevRun in Directory.Build.targets); the brokers read plugins from there
    and nowhere else (Surgewave:PluginsDirectory), so a plugin installed for this user or machine never replaces the
    one the checkout built. A start without the staged plugin is refused: the broker would close every Kafka client.

    start makes the brokers' data afresh (data/ is emptied); restart keeps it. A cluster's brokers know each other
    from the start (Surgewave:ClusterNodes) and declare the controller quorum (Surgewave:ControllerQuorumVoters,
    #172): every broker votes.

    A start at an offset whose stack runs already, complete, with the same number of brokers, and every part
    answering, uses it as it is: it says so and returns 0, without building, stopping or touching its data. A stack
    that runs only in part, or with another number of brokers, is stopped and started afresh. restart always stops
    and starts. start, restart and stop of one stack take turns: a second start while the first still waits for its
    parts waits for it (a named mutex, no pause) and then uses the stack the first one started.

    start returns once every part answers: a broker GET /health/live with 200 on its REST port, the Control UI GET /
    with 200. The wait is a handshake: each part's console signals when the host says it has started, or the console
    ends; no pause, no polling. All parts start at once, since a broker of a cluster waits for its peers. A part that
    ends before it answers fails the start with the tail of its log, and the whole stack is stopped; one that does
    not get there within -ReadyDeadline fails it too. That a cluster has elected its controller is not part of the
    handshake: status shows each broker's Raft state and leader.

    A stack never stops a process it did not start: it knows its parts by the consoles whose command lines name this
    checkout's scripts/stack/part.ps1 and the part's record in the stack's folder, or, with a console gone, by the
    record of the part's process id, program and start time.

.PARAMETER Verb
    start, stop, restart, status, log or build.

.PARAMETER Offset
    The stack's port offset: every port of the stack moves by it (see the ports above). Default:
    SURGEWAVE_STACK_OFFSET, else 0. start and restart take multiples of 100 only: a stack of up to ten brokers takes
    ports ten apart from its offset on, so offsets 100 apart never share one. A port held by anything that is not this
    stack is refused, naming its holder, and the stack of this checkout that holds it if one does. status without an
    offset shows every running stack of this checkout.

.PARAMETER Hidden
    start: no console windows. A stack's consoles are always hidden here, each part's output going to its log file
    only (stack log -Part); the switch is taken so that a caller passes it as it does to Terracle's and Modelzero's
    scripts, where it means the same.

.PARAMETER Brokers
    start: how many brokers (default 1, at most 10). 3 is the useful cluster: it survives the loss of one broker.
    restart keeps the number start was given.

.PARAMETER Build
    start, restart: build first, always.

.PARAMETER NoBuild
    start, restart: never build; a stale build is started, and the start says it is stale and why. Without -Build or
    -NoBuild, start and restart build when the build output is missing or older than the checkout.

.PARAMETER ReadyDeadline
    start, restart: how many seconds the parts have to answer (default 120). The bound only decides when a part that
    neither answers nor ends counts as failed; the stack is stopped then.

.PARAMETER Part
    log: control, or broker1, broker2 and on: that part's output at the offset. Without it, the life log.

.PARAMETER Tail
    log: how many lines from the end (default 40).

.PARAMETER Follow
    log: keep showing what is written.

.PARAMETER Force
    stop: end the parts from outside at once, without Ctrl+C first, for a part known to hang.

.OUTPUTS
    The exit code. status: 0 when the stack at the offset (without -Offset: every running stack of this checkout)
    runs complete, every part answering; 1 when nothing of it runs; 2 when it runs in part: a part is missing, or a
    running part does not answer. Every other verb: 0 when it did what it was asked, 1 when it failed.

.EXAMPLE
    ./scripts/stack.ps1 start
    One broker, Kafka bootstrap localhost:9092, the Control UI at http://localhost:5050.

.EXAMPLE
    ./scripts/stack.ps1 start -Offset 100 -Brokers 3
    A cluster of three, bootstrap localhost:9192,localhost:9202,localhost:9212, the Control UI at
    http://localhost:5150; stop it with ./scripts/stack.ps1 stop -Offset 100.

.EXAMPLE
    ./scripts/stack.ps1 log -Part broker2 -Follow
    Broker 2's output at offset 0, as it is written.
#>
[CmdletBinding(PositionalBinding = $false)]
param(
    [Parameter(Position = 0)] [ValidateSet('start', 'stop', 'restart', 'status', 'log', 'build')] [string] $Verb,
    [ValidateRange(0, 55000)] [int] $Offset,
    [ValidateRange(1, 10)] [int] $Brokers = 1,
    [switch] $Hidden,
    [switch] $Build,
    [switch] $NoBuild,
    [ValidateRange(1, 3600)] [int] $ReadyDeadline = 120,
    [ValidatePattern('^(control|broker([1-9]|10))$')] [string] $Part,
    [ValidateRange(1, 100000)] [int] $Tail = 40,
    [switch] $Follow,
    [switch] $Force
)

$ErrorActionPreference = 'Stop'

if (-not $Verb) {
    Get-Help $PSCommandPath
    return
}

# Which parameter belongs to which verb: a parameter another verb would take is refused, not ignored.
$verbParameters = @{
    start   = @('Offset', 'Brokers', 'Hidden', 'Build', 'NoBuild', 'ReadyDeadline')
    stop    = @('Offset', 'Force')
    restart = @('Offset', 'Build', 'NoBuild', 'ReadyDeadline')
    status  = @('Offset')
    log     = @('Offset', 'Part', 'Tail', 'Follow')
    build   = @()
}
$common = [System.Management.Automation.PSCmdlet]::CommonParameters + [System.Management.Automation.PSCmdlet]::OptionalCommonParameters
foreach ($name in $PSBoundParameters.Keys) {
    if ($name -eq 'Verb' -or $common -contains $name) { continue }
    if ($verbParameters[$Verb] -notcontains $name) {
        $takes = if ($verbParameters[$Verb].Count -gt 0) { ($verbParameters[$Verb] | ForEach-Object { "-$_" }) -join ', ' } else { 'no parameter' }
        throw "-$name does not belong to 'stack $Verb' (it takes: $takes)."
    }
}
if ($Build -and $NoBuild) { throw '-Build and -NoBuild contradict each other.' }
# The processes are found through Windows' process list (command lines, parents, start times).
if (-not $IsWindows) { throw 'The stack runs on Windows.' }

$Repo = Split-Path -Parent $PSScriptRoot
$Artifacts = Join-Path $Repo 'artifacts'
# A relative path means one from PowerShell's location; [IO.Path]::GetFullPath would read it from the process's folder,
# which Set-Location does not move.
$StacksRoot = if ($env:SURGEWAVE_STACK_ROOT) { $PSCmdlet.GetUnresolvedProviderPathFromPSPath($env:SURGEWAVE_STACK_ROOT) } else { Join-Path $Artifacts 'stacks' }
# The two hosts the stack runs, their build output and their program.
$Programs = [ordered]@{
    broker  = [pscustomobject]@{ Project = 'src/Kuestenlogik.Surgewave.Broker.App/Kuestenlogik.Surgewave.Broker.App.csproj'; Output = Join-Path $Artifacts 'bin/Kuestenlogik.Surgewave.Broker.App/Debug'; Exe = 'surgewave-broker.exe' }
    control = [pscustomobject]@{ Project = 'src/Kuestenlogik.Surgewave.Control/Kuestenlogik.Surgewave.Control.csproj'; Output = Join-Path $Artifacts 'bin/Kuestenlogik.Surgewave.Control/Debug'; Exe = 'surgewave-control.exe' }
}
$BuildOutputs = @($Programs.Values | ForEach-Object { $_.Output })
$BuildStamp = Join-Path $Artifacts 'stack-build.stamp'
$BuildLog = Join-Path $Artifacts 'stack-build.log'
# The Kafka wire protocol is a plugin (#59); the build stages it here for a run from the build output.
$KafkaPlugin = 'plugins/kuestenlogik.surgewave.protocol.kafka'
# How far apart the brokers of one stack are: each broker's ports are those of the one before it plus this.
$BrokerPortStep = 10
$PartScript = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot 'stack/part.ps1'))
. (Join-Path $PSScriptRoot 'stack/log.ps1')
. (Join-Path $PSScriptRoot 'stack/console.ps1')
. (Join-Path $PSScriptRoot 'stack/leftovers.ps1')
. (Join-Path $PSScriptRoot 'stack/staleness.ps1')
. (Join-Path $PSScriptRoot 'stack/settings.ps1')

$offsetNamed = $PSBoundParameters.ContainsKey('Offset')
if (-not $offsetNamed -and $env:SURGEWAVE_STACK_OFFSET) {
    $Offset = [int]$env:SURGEWAVE_STACK_OFFSET
    $offsetNamed = $true
}
# A stack of up to ten brokers takes ports ten apart from its offset on; offsets a hundred apart keep any two stacks
# off each other's.
if ($Verb -in 'start', 'restart' -and $Offset % 100 -ne 0) {
    throw "Stack offsets are multiples of 100, so no two stacks share a port; $Offset is not."
}

# ── where a stack lives ─────────────────────────────────────────────────────────────────────────────────────

function Read-JsonFile([string] $File) { Get-Content -LiteralPath $File -Raw | ConvertFrom-Json -AsHashtable }

# The port of an endpoint URL; an explicit port only, since a URL without one would leave the stack guessing.
function Split-EndpointUrl([string] $Url, [string] $What) {
    if ("$Url" -notmatch '^(?<scheme>[a-z]+)://(?<host>\[[^\]]+\]|[^/:]+):(?<port>\d+)/?$') {
        throw "$What names no port (it says '$Url'); the stack moves the parts' own ports and needs one."
    }
    [pscustomobject]@{ Scheme = $Matches.scheme; Host = $Matches.host; Port = [int]$Matches.port }
}

<#
    The stack's layout: every port of every part, each the part's own as the appsettings.json of its build names it,
    moved by the offset (and a broker by $BrokerPortStep more for each broker before it). Read from the build, not
    written down here, so a port changed in a part's configuration moves every stack with it.
#>
function Get-StackLayout([int] $At, [int] $Count) {
    $brokerFile = Join-Path $Programs.broker.Output 'appsettings.json'
    $controlFile = Join-Path $Programs.control.Output 'appsettings.json'
    foreach ($file in $brokerFile, $controlFile) {
        if (-not (Test-Path -LiteralPath $file)) { throw "The stack's ports come from the parts' appsettings.json, and there is none at $file. Build first: ./scripts/stack.ps1 build." }
    }
    $broker = Read-JsonFile $brokerFile
    $surgewave = $broker.Surgewave
    foreach ($key in 'Host', 'Port', 'GrpcPort', 'ReplicationPort') {
        if ($null -eq $surgewave.$key) { throw "$brokerFile names no Surgewave:$key; the stack takes it from there." }
    }
    # The broker serves gRPC and REST on GrpcPort, over https while GrpcUseTls is on (its Program.cs then binds the
    # Grpc endpoint to https://*:GrpcPort), else as its Kestrel Grpc endpoint says.
    $endpoints = $broker.Kestrel.Endpoints
    $restScheme = if ($surgewave.GrpcUseTls) { 'https' } elseif ($endpoints.Grpc.Url) { (Split-EndpointUrl $endpoints.Grpc.Url "$brokerFile, Kestrel:Endpoints:Grpc:Url").Scheme } else { 'http' }
    # The PostgreSQL wire listener opens only when the configuration switches it on; then its port moves too.
    $postgreSql = if ($surgewave.PostgreSql.Enabled) {
        if ($null -eq $surgewave.PostgreSql.Port) { throw "$brokerFile switches Surgewave:PostgreSql on and names no Surgewave:PostgreSql:Port." }
        [int]$surgewave.PostgreSql.Port
    }
    $control = Read-JsonFile $controlFile
    $ui = Split-EndpointUrl $control.Kestrel.Endpoints.Http.Url "$controlFile, Kestrel:Endpoints:Http:Url"

    $layout = [ordered]@{
        host       = [string]$surgewave.Host
        bootstrap  = $null
        controlUrl = "$($ui.Scheme)://localhost:$($ui.Port + $At)"
        brokers    = @(foreach ($n in 1..$Count) {
            $shift = $At + ($n - 1) * $BrokerPortStep
            $moved = [ordered]@{}
            # HTTP/3 runs over QUIC, which takes the endpoint's port on UDP as well.
            $udp = @()
            foreach ($name in @($endpoints.Keys | Sort-Object)) {
                $url = Split-EndpointUrl $endpoints[$name].Url "$brokerFile, Kestrel:Endpoints:${name}:Url"
                $moved[$name] = '{0}://{1}:{2}' -f $url.Scheme, $url.Host, ($url.Port + $shift)
                if ("$($endpoints[$name].Protocols)" -match 'Http3') { $udp += $url.Port + $shift }
            }
            $grpc = [int]$surgewave.GrpcPort + $shift
            [ordered]@{
                name        = "broker$n"
                id          = $n
                kafka       = [int]$surgewave.Port + $shift
                grpc        = $grpc
                replication = [int]$surgewave.ReplicationPort + $shift
                postgreSql  = if ($postgreSql) { $postgreSql + $shift } else { $null }
                endpoints   = $moved
                udp         = $udp
                rest        = "${restScheme}://localhost:$grpc"
            }
        })
        control    = [ordered]@{
            name     = 'control'
            port     = $ui.Port + $At
            endpoint = '{0}://{1}:{2}' -f $ui.Scheme, $ui.Host, ($ui.Port + $At)
            url      = "$($ui.Scheme)://localhost:$($ui.Port + $At)"
        }
    }
    $layout.bootstrap = ($layout.brokers | ForEach-Object { "$($layout.host):$($_.kafka)" }) -join ','
    # The step keeps the brokers apart only while a broker's ports lie close together; a configuration that spreads
    # them is refused here rather than found as a port two parts fight over.
    $all = @(Get-StackLayoutPorts $layout)
    $twice = @($all | Group-Object Port | Where-Object Count -gt 1)
    if ($twice.Count -gt 0) {
        throw "The parts' configuration puts two of the stack's ports on one: $(($twice | ForEach-Object { "$($_.Name) ($(($_.Group.What) -join ', '))" }) -join '; ')."
    }
    $layout
}

# Every port of a layout, with what takes it and whether it takes UDP as well (HTTP/3).
function Get-StackLayoutPorts([System.Collections.IDictionary] $Layout) {
    foreach ($b in $Layout.brokers) {
        [pscustomobject]@{ Port = [int]$b.kafka; What = "$($b.name) Kafka"; Udp = $false }
        [pscustomobject]@{ Port = [int]$b.replication; What = "$($b.name) replication"; Udp = $false }
        $grpcListed = $false
        foreach ($e in $b.endpoints.GetEnumerator()) {
            $port = (Split-EndpointUrl $e.Value $e.Key).Port
            if ($port -eq [int]$b.grpc) { $grpcListed = $true }
            [pscustomobject]@{ Port = $port; What = "$($b.name) $($e.Key)"; Udp = @($b.udp) -contains $port }
        }
        if (-not $grpcListed) { [pscustomobject]@{ Port = [int]$b.grpc; What = "$($b.name) gRPC"; Udp = $false } }
        if ($b.postgreSql) { [pscustomobject]@{ Port = [int]$b.postgreSql; What = "$($b.name) PostgreSQL"; Udp = $false } }
    }
    [pscustomobject]@{ Port = [int]$Layout.control.port; What = 'control UI'; Udp = $false }
}

function Get-StackDir([int] $At) { Join-Path $StacksRoot "$At" }

function Get-PartLog([int] $At, [string] $Which) { Join-Path (Get-StackDir $At) "logs/$Which.log" }

function Get-PartRecord([int] $At, [string] $Which) { Join-Path (Get-StackDir $At) "parts/$Which.json" }

function Get-PartProgram([string] $Which) { if ($Which -eq 'control') { $Programs.control } else { $Programs.broker } }

# Where the log of a part's last run goes when it starts again: <part>.previous.log, which the next start replaces;
# a run that failed (its end line names an exit code that is neither a clean end, Ctrl+C nor a stop from outside) is
# kept as <part>.failed-<when it ended>.log instead, so a restart after a crash never takes the crash's own output
# with it.
function Get-PartLogKept([string] $LogFile) {
    $ended = Select-String -LiteralPath $LogFile -Pattern '^---- .* ended with exit code (-?\d+) after ' | Select-Object -Last 1
    $code = if ($ended) { [int]$ended.Matches[0].Groups[1].Value } else { 0 }
    if ($code -in 0, -1, -1073741510) { return $LogFile -replace '\.log$', '.previous.log' }
    $when = (Get-Item -LiteralPath $LogFile).LastWriteTime.ToString('yyyyMMdd-HHmmss')
    $LogFile -replace '\.log$', ".failed-$when.log"
}

function Read-StackState([int] $At) {
    $file = Join-Path (Get-StackDir $At) 'stack.json'
    if (Test-Path $file) { Read-JsonFile $file }
}

function Write-StackState([int] $At, [System.Collections.IDictionary] $State) {
    $dir = Get-StackDir $At
    New-Item -ItemType Directory -Force $dir | Out-Null
    $State | ConvertTo-Json -Depth 6 | Set-Content -Path (Join-Path $dir 'stack.json') -Encoding utf8
}

# ── the parts of this checkout's stacks ─────────────────────────────────────────────────────────────────────

<#
    Opens the process a snapshot of the process list saw, and keeps it open: a [Diagnostics.Process] with its handle
    taken, and only when its start time is the snapshot's (or the recorded one), so a process that got a freed id
    between the snapshot and the opening is never taken for it. While the handle is open Windows does not give the
    id to another process, so every later wait, Ctrl+C and end reaches this process and no other. $null when it has
    ended or is another.
#>
function Open-StackProcess([int] $Id, [datetime] $Started) {
    try {
        $p = [System.Diagnostics.Process]::GetProcessById($Id)
        $null = $p.Handle
    } catch { return $null }
    try {
        if ([Math]::Abs(($p.StartTime.ToUniversalTime() - $Started.ToUniversalTime()).TotalSeconds) -gt 1) { $p.Dispose(); return $null }
    } catch { $p.Dispose(); return $null }
    $p
}

<#
    The parts of every stack of this checkout, or of one offset: a pwsh running this checkout's scripts/stack/part.ps1
    whose command line names the part's record file in this stacks root (so a stack of another checkout or another
    stacks root is never touched), with the program it started (its child). A console whose part is gone is listed
    without a process. A part whose console is gone (ended from outside while the part ran on) is found through its
    record, the same process id, program and start time. Consoles and parts are opened at once (Open-StackProcess)
    and carried as process objects: what is stopped later is what was found here.
#>
function Get-StackParts([Nullable[int]] $Only) {
    $all = @(Get-CimInstance Win32_Process)
    $found = foreach ($c in $all | Where-Object { $_.Name -eq 'pwsh.exe' -and $_.CommandLine -and $_.CommandLine.IndexOf($PartScript, [StringComparison]::OrdinalIgnoreCase) -ge 0 }) {
        if ($c.CommandLine -notmatch '-Offset\s+"?(\d+)') { continue }
        $at = [int]$Matches[1]
        if ($c.CommandLine -notmatch '-Part\s+"?([a-z0-9]+)') { continue }
        $which = $Matches[1]
        if ($null -ne $Only -and $at -ne $Only) { continue }
        if ($c.CommandLine.IndexOf((Get-PartRecord $at $which), [StringComparison]::OrdinalIgnoreCase) -lt 0) { continue }
        $console = Open-StackProcess ([int]$c.ProcessId) $c.CreationDate
        if (-not $console) { continue }
        $exe = (Get-PartProgram $which).Exe
        # A child starts after its parent; an older process under the parent's id as its parent id is not the console's.
        $child = $all | Where-Object { $_.ParentProcessId -eq $c.ProcessId -and $_.Name -eq $exe -and $_.CreationDate -ge $c.CreationDate } | Select-Object -First 1
        $process = if ($child) { Open-StackProcess ([int]$child.ProcessId) $child.CreationDate }
        [pscustomobject]@{ Console = $console; Offset = $at; Part = $which; Process = $process }
    }
    $found
    $listed = @($found | Where-Object { $_.Process } | ForEach-Object { $_.Process.Id })
    $pattern = if ($null -ne $Only) { Join-Path (Get-StackDir $Only) 'parts/*.json' } else { Join-Path $StacksRoot '*/parts/*.json' }
    foreach ($record in @(Get-ChildItem -Path $pattern -ErrorAction SilentlyContinue)) {
        try { $part = Get-Content -Raw $record.FullName | ConvertFrom-Json } catch { continue }
        if ($listed -contains [int]$part.pid) { continue }
        $process = $all | Where-Object { $_.ProcessId -eq [int]$part.pid } | Select-Object -First 1
        if (-not $process -or -not $process.ExecutablePath -or [IO.Path]::GetFullPath($process.ExecutablePath) -ine [IO.Path]::GetFullPath($part.exe)) { continue }
        $opened = Open-StackProcess ([int]$part.pid) ([DateTime]::new([long]$part.startedUtcTicks, [DateTimeKind]::Utc))
        if (-not $opened) { continue }
        [pscustomobject]@{ Console = $null; Offset = [int]$record.Directory.Parent.Name; Part = $record.BaseName; Process = $opened }
    }
}

function Format-StackConsole($Found) { if ($Found.Console) { "console $($Found.Console.Id)" } else { 'its console gone' } }

# The order a stack stops in: the Control UI first, then the brokers from the last to the first.
function Get-PartRank([string] $Which) { if ($Which -eq 'control') { 0 } else { 1000 - [int]($Which -replace '\D', '') } }

# ── stop ────────────────────────────────────────────────────────────────────────────────────────────────────

# Ends processes from outside and awaits each exit. It takes the process objects Get-StackParts opened, never ids:
# an id looked up again could name a process that started since.
function Stop-AndWait([System.Diagnostics.Process[]] $Processes) {
    foreach ($p in @($Processes | Where-Object { $_ })) {
        try { if ($p.HasExited) { continue } } catch { continue }
        try { $p.Kill() } catch { }
        # The exit itself is the signal; no fixed pause.
        try { $p.WaitForExit() } catch { }
    }
}

<#
    Stops every part of the stack at an offset, their consoles included: Ctrl+C in each part's console, as a person
    would press it, and its exit awaited; only a part that does not end within the bound, or with -Hard, is ended
    from outside. The Control UI goes first, then the brokers from the last to the first, one after the other. Then
    the consoles that are left are awaited or closed. Every step goes to the life log. Last, every process of the
    parts that still runs is named and left in $script:StackLeftovers for the verb to fail on; nothing is ended for
    it. Returns whether anything of the stack ran.
#>
function Stop-StackParts([int] $At, [switch] $Hard) {
    $script:StackLeftovers = @()
    $parts = @(Get-StackParts $At)
    $targets = @($parts | Where-Object { $_.Process } | Sort-Object { Get-PartRank $_.Part })
    if ($parts.Count -eq 0) {
        Write-Host "  nothing running at offset $At" -ForegroundColor DarkGray
        return $false
    }
    $tree = @(Get-StackProcessTree -Roots @($targets | ForEach-Object { $_.Process.Id }))
    $how = if ($Hard) { 'from outside (-Force)' } else { 'with Ctrl+C in its console' }
    $outside = @()
    foreach ($t in $targets) {
        $name = "$($t.Process.ProcessName).exe"
        Write-Host ("  stopping {0} (pid {1})" -f $t.Part, $t.Process.Id) -ForegroundColor Yellow
        Write-StackLog -Source 'stack' ("stopping {0} pid={1} ({2} at offset {3}, {4}) {5}" -f $name, $t.Process.Id, $t.Part, $At, (Format-StackConsole $t), $how)
        if ($Hard) { $outside += $t.Process; continue }
        # Ctrl+C through a process of its own without a console; its answer comes when the part has ended. This script
        # holds the part's and the console's handles meanwhile, so the ids the helper is given name no other process.
        $consoleId = if ($t.Console) { $t.Console.Id } else { 0 }
        $command = "& { ${function:Send-StackCtrlC} } -ConsolePid $consoleId -PartPid $($t.Process.Id)"
        $psi = [System.Diagnostics.ProcessStartInfo]::new((Get-Process -Id $PID).Path)
        foreach ($a in @('-NoProfile', '-NonInteractive', '-EncodedCommand', [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($command)))) { $psi.ArgumentList.Add($a) }
        $psi.UseShellExecute = $false
        $psi.CreateNoWindow = $true
        $psi.RedirectStandardOutput = $true
        $psi.RedirectStandardError = $true
        $helper = [System.Diagnostics.Process]::Start($psi)
        $answer = ($helper.StandardOutput.ReadToEnd() + $helper.StandardError.ReadToEnd()).Trim()
        $helper.WaitForExit()
        if ($answer -match 'ended (-?\d+) after (\d+) ms') {
            $line = "{0} pid={1} ({2} at offset {3}) ended in order after Ctrl+C: exit code {4} after {5} s" -f $name, $t.Process.Id, $t.Part, $At, $Matches[1], ([int]$Matches[2] / 1000).ToString('0.0', [cultureinfo]::InvariantCulture)
            Write-Host "  $line" -ForegroundColor DarkGray
            Write-StackLog -Source 'stack' $line
        } elseif ($answer -like 'failed: the part is not running*' -or $t.Process.HasExited) {
            # It ended on its own between the finding and the Ctrl+C: nothing to end.
            Write-Host "  $($t.Part) had ended already" -ForegroundColor DarkGray
            Write-StackLog -Source 'stack' ("{0} pid={1} ({2} at offset {3}) had ended before the Ctrl+C" -f $name, $t.Process.Id, $t.Part, $At)
        } else {
            $why = if ($answer -eq 'timeout') { 'did not end within the bound after Ctrl+C' } else { "Ctrl+C not delivered ($answer)" }
            Write-Host "  $($t.Part) $why; ending it from outside" -ForegroundColor Yellow
            Write-StackLog -Source 'stack' ("{0} pid={1} ({2} at offset {3}) {4}; ending it from outside" -f $name, $t.Process.Id, $t.Part, $At, $why)
            $outside += $t.Process
        }
    }
    foreach ($p in $outside) { if (-not $p.HasExited) { Write-StackLog -Source 'stack' "terminating pid=$($p.Id) from outside" } }
    Stop-AndWait $outside

    # A console closes by itself once its part has ended, and its last act is the part's end in the life log: its own
    # exit is the signal. The bound only guards against a console that hangs; one without a part is closed.
    foreach ($s in $parts) {
        if (-not $s.Console) {
            Remove-Item -LiteralPath (Get-PartRecord $At $s.Part) -ErrorAction SilentlyContinue
            continue
        }
        if ($s.Console.HasExited) { continue }
        if ($s.Process -and $s.Console.WaitForExit(15000)) { continue }
        Write-StackLog -Source 'stack' "closing console $($s.Console.Id) ($($s.Part) at offset $At)"
        Stop-AndWait @($s.Console)
    }

    foreach ($left in @(Find-StackLeftovers $tree)) {
        $p = if ($left.Started) { Open-StackProcess $left.ProcessId $left.Started }
        if ($p) { try { [void]$p.WaitForExit(15000) } catch { } }
    }
    $script:StackLeftovers = @(Find-StackLeftovers $tree)
    foreach ($left in $script:StackLeftovers) {
        $line = "{0} pid={1} still runs after the stop (stack at offset {2}); end it by its pid once it is known why" -f $left.Name, $left.ProcessId, $At
        Write-Host "  $line" -ForegroundColor Red
        Write-StackLog -Source 'stack' $line
    }
    return $true
}

# ── build ───────────────────────────────────────────────────────────────────────────────────────────────────

# The two hosts in Debug, one after the other: the broker's build builds the Kafka plugin first and stages it into
# its plugins/ folder. Their output goes to artifacts/stack-build.log; a failure lists the distinct errors. The stacks
# run from copies, so a build never meets their files. A build that succeeds leaves its stamp (Write-StackBuildStamp):
# what it began from and when, so a file written while it ran still counts as newer than the build.
function Invoke-StackBuild {
    $began = [DateTime]::UtcNow
    # What the build is about to see: the stamp names it, so a later change is told apart from what the build held.
    $sources = Get-StackSources $Repo
    New-Item -ItemType Directory -Force $Artifacts | Out-Null
    Set-Content -LiteralPath $BuildLog -Value "stack build $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -Encoding utf8
    foreach ($program in $Programs.Values) {
        Write-Host "Building $($program.Project)..." -ForegroundColor Cyan
        Write-StackLog -Source 'stack' "building $($program.Project)"
        & dotnet build (Join-Path $Repo $program.Project) -c Debug --nologo 2>&1 | Add-Content -LiteralPath $BuildLog -Encoding utf8
        if ($LASTEXITCODE -ne 0) {
            $code = $LASTEXITCODE
            Write-StackLog -Source 'stack' "build of $($program.Project) failed (exit code $code)"
            $errors = @(Select-String -LiteralPath $BuildLog -Pattern ': error ' | ForEach-Object { $_.Line.Trim() } | Sort-Object -Unique)
            $errors | Select-Object -First 20 | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
            throw "The build of $($program.Project) failed (exit code $code); $([IO.Path]::GetRelativePath($Repo, $BuildLog)) has its whole output."
        }
    }
    Write-StackBuildStamp -Stamp $BuildStamp -Began $began -Sources $sources -Output $BuildOutputs
    Write-Host 'Built.' -ForegroundColor Green
}

# Builds when asked to (-Build), or when the build is stale and building is not ruled out (-NoBuild); a stale build
# started with -NoBuild is said, not hidden.
function Update-StackBuild([bool] $Always, [bool] $Never) {
    if ($Always) { Invoke-StackBuild; return }
    $stale = Get-StackBuildStaleness -Repo $Repo -Output $BuildOutputs -Stamp $BuildStamp
    if (-not $stale) { return }
    if ($Never) {
        Write-Host "The build is stale: $stale. Starting it as it is (-NoBuild)." -ForegroundColor Yellow
        Write-StackLog -Source 'stack' "the build is stale ($stale); starting it as it is (-NoBuild)"
        return
    }
    Write-Host "The build is stale: $stale." -ForegroundColor Cyan
    Write-StackLog -Source 'stack' "the build is stale ($stale); building"
    Invoke-StackBuild
}

# ── start ───────────────────────────────────────────────────────────────────────────────────────────────────

# A port held by anything else (another checkout's stack, a stack at another offset, a foreign program) would let
# the handshake answer with someone else's part. This stack's own parts are stopped before, so whatever still listens
# is not ours: refuse, naming the holder. Get-NetTCPConnection reads the state as a value, not as localised netstat
# text. UDP is checked only for an endpoint that serves HTTP/3: other programs hold UDP ports a TCP endpoint never
# meets (Windows' Connected Devices Platform holds 5050/udp).
function Assert-PortsFree([int] $At, [System.Collections.IDictionary] $Layout) {
    $stacks = $null
    $busy = @(foreach ($entry in @(Get-StackLayoutPorts $Layout)) {
        $owner = (Get-NetTCPConnection -LocalPort $entry.Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1).OwningProcess
        if (-not $owner -and $entry.Udp) { $owner = (Get-NetUDPEndpoint -LocalPort $entry.Port -ErrorAction SilentlyContinue | Select-Object -First 1).OwningProcess }
        if ($owner) {
            $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $owner" -ErrorAction SilentlyContinue
            if ($null -eq $stacks) { $stacks = @(Get-StackParts $null | Where-Object { $_.Process }) }
            $stack = $stacks | Where-Object { $_.Process.Id -eq $owner } | Select-Object -First 1
            $which = if ($stack) { ", $($stack.Part) of this checkout's stack at offset $($stack.Offset)" } else { '' }
            "  {0,-5} ({1}) held by {2} (pid {3}) {4}{5}" -f $entry.Port, $entry.What, $proc.Name, $owner, $proc.ExecutablePath, $which
        }
    })
    if ($busy.Count -gt 0) {
        Write-Host "Ports the stack at offset $At needs are taken by something that is not this stack:" -ForegroundColor Red
        $busy | ForEach-Object { Write-Host $_ -ForegroundColor Red }
        throw 'Ports busy. Pick another -Offset, or stop the stack that holds them, from its own checkout when it is another.'
    }
}

function Show-PartLogTail([int] $At, [string] $Which, [int] $Lines = 30) {
    $file = Get-PartLog $At $Which
    if (-not (Test-Path -LiteralPath $file)) { return }
    Write-Host "The last lines of $([IO.Path]::GetRelativePath($Repo, $file)):" -ForegroundColor Yellow
    Get-Content -LiteralPath $file -Tail $Lines | ForEach-Object { Write-Host "  $_" }
}

# One request that says whether the part that signalled is the one that answers, and with 200.
function Test-PartAnswer([System.Collections.IDictionary] $Layout, [string] $Which, [int] $TimeoutSec) {
    $url = if ($Which -eq 'control') { "$($Layout.control.url)/" } else { "$(($Layout.brokers | Where-Object { $_.name -eq $Which }).rest)/health/live" }
    try {
        $answer = Invoke-WebRequest -Uri $url -TimeoutSec $TimeoutSec -SkipHttpErrorCheck -MaximumRedirection 0
        if ($answer.StatusCode -ne 200) { return "GET $url answered $($answer.StatusCode)" }
    } catch {
        return "GET $url failed: $($_.Exception.Message)"
    }
    $null
}

# Copies a build output into the stack: made afresh, so a restart runs the latest build.
function Copy-PartBuild([string] $From, [string] $To) {
    if (Test-Path -LiteralPath $To) { Remove-Item -LiteralPath $To -Recurse -Force }
    New-Item -ItemType Directory -Force $To | Out-Null
    Copy-Item -Path (Join-Path $From '*') -Destination $To -Recurse -Force
}

<#
    Starts every part of a stack whose own parts are stopped: the ports checked, the build copied, the data prepared,
    the settings written, the consoles started, and the handshakes awaited. $FreshData empties data/ (start); a
    restart keeps it.
#>
function Start-StackParts([System.Collections.IDictionary] $State, [bool] $FreshData, [int] $Deadline) {
    $at = $State.offset
    $dir = Get-StackDir $at
    $layout = Get-StackLayout $at $State.brokerCount
    # stack.json holds the layout at its top level, for other scripts to read (see the help).
    foreach ($key in @($layout.Keys)) { $State[$key] = $layout[$key] }
    Assert-PortsFree $at $layout

    foreach ($program in $Programs.Values) {
        if (-not (Test-Path (Join-Path $program.Output $program.Exe))) { throw "Not built: $(Join-Path $program.Output $program.Exe). Start without -NoBuild, or run ./scripts/stack.ps1 build." }
    }
    if (-not (Test-Path (Join-Path $Programs.broker.Output $KafkaPlugin))) {
        throw "The broker's build output has no Kafka protocol plugin at $KafkaPlugin, and without it the broker closes every Kafka client. Build again: ./scripts/stack.ps1 build."
    }

    $bins = @{ broker = Join-Path $dir 'bin/broker'; control = Join-Path $dir 'bin/control' }
    Copy-PartBuild $Programs.broker.Output $bins.broker
    Copy-PartBuild $Programs.control.Output $bins.control

    $data = Join-Path $dir 'data'
    if ($FreshData -and (Test-Path -LiteralPath $data)) { Remove-Item -LiteralPath $data -Recurse -Force }

    # Each part: its settings, its data folder, its log moved aside.
    $parts = @(foreach ($b in $layout.brokers) {
        $partData = Join-Path $data $b.name
        [pscustomobject]@{ Name = $b.name; Bin = $bins.broker; Exe = Join-Path $bins.broker $Programs.broker.Exe; Data = $partData
            Settings = New-StackBrokerSettings -Layout $layout -Broker $b -DataDirectory $partData -PluginsDirectory (Join-Path $bins.broker 'plugins') }
    }) + [pscustomobject]@{ Name = 'control'; Bin = $bins.control; Exe = Join-Path $bins.control $Programs.control.Exe; Data = Join-Path $data 'control'
        Settings = New-StackControlSettings -Layout $layout -DataDirectory (Join-Path $data 'control') -ContentRoot $bins.control }
    New-Item -ItemType Directory -Force (Join-Path $dir 'env'), (Join-Path $dir 'logs') | Out-Null
    foreach ($p in $parts) {
        New-Item -ItemType Directory -Force $p.Data | Out-Null
        $p | Add-Member EnvFile (Join-Path $dir "env/$($p.Name).json")
        $p.Settings | ConvertTo-Json | Set-Content -Path $p.EnvFile -Encoding utf8
        $log = Get-PartLog $at $p.Name
        if (Test-Path -LiteralPath $log) { Move-Item -Force -LiteralPath $log -Destination (Get-PartLogKept $log) }
    }
    $State.startedAt = (Get-Date).ToString('o')
    Write-StackState $at $State

    Write-Host ("Starting the stack at offset {0}: {1} broker(s) and the Control UI..." -f $at, $layout.brokers.Count) -ForegroundColor Cyan
    # Every part at once: a broker of a cluster waits for its peers before it is ready. Each console sets an event of
    # its own when its part says it has started; its own exit ends its wait as well. A name of its own for every
    # start, so a console of an earlier start can never answer for this one.
    $waits = [System.Collections.Generic.List[object]]::new()
    try {
        foreach ($p in $parts) {
            $readyName = "Local\SurgewaveStack-$at-$($p.Name)-$([guid]::NewGuid().ToString('N'))"
            $ready = [System.Threading.EventWaitHandle]::new($false, [System.Threading.EventResetMode]::ManualReset, $readyName)
            $label = "$($p.Name) (offset $at)"
            # Double quotes around every value: Start-Process joins the arguments into one command line as they are.
            $arguments = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', "`"$PartScript`"", '-Offset', $at, '-Part', $p.Name,
                '-Exe', "`"$($p.Exe)`"", '-EnvFile', "`"$($p.EnvFile)`"", '-LogFile', "`"$(Get-PartLog $at $p.Name)`"", '-RecordFile', "`"$(Get-PartRecord $at $p.Name)`"",
                '-ReadyEvent', "`"$readyName`"", '-Label', "`"$label`"")
            $console = Start-Process (Get-Process -Id $PID).Path -ArgumentList ($arguments -join ' ') -WorkingDirectory $p.Bin -WindowStyle Hidden -PassThru
            Write-StackLog -Source 'stack' "console $($console.Id) opened for $label, hidden"
            $exited = [System.Threading.ManualResetEvent]::new($false)
            $exited.SafeWaitHandle = [Microsoft.Win32.SafeHandles.SafeWaitHandle]::new($console.Handle, $false)
            $waits.Add([pscustomobject]@{ Part = $p.Name; Ready = $ready; Exited = $exited; Console = $console; Signalled = $false })
        }

        $watch = [System.Diagnostics.Stopwatch]::StartNew()
        $problem = $null
        $failed = $null
        # The Control UI's page asks broker 1 while it renders, so its one request waits until every broker answers:
        # a Control that has started is held as signalled, its console's exit still watched, until then.
        while ($waits.Count -gt 0 -and -not $problem) {
            $brokersLeft = @($waits | Where-Object { $_.Part -ne 'control' }).Count
            $w = $waits | Where-Object { $_.Signalled -and $brokersLeft -eq 0 } | Select-Object -First 1
            if (-not $w) {
                $left = [TimeSpan]::FromSeconds($Deadline) - $watch.Elapsed
                if ($left -lt [TimeSpan]::Zero) { $left = [TimeSpan]::Zero }
                $listening = @($waits | Where-Object { -not $_.Signalled })
                $handles = [System.Threading.WaitHandle[]]@(@($listening | ForEach-Object { $_.Ready }) + @($waits | ForEach-Object { $_.Exited }))
                $signal = [System.Threading.WaitHandle]::WaitAny($handles, $left)
                if ($signal -eq [System.Threading.WaitHandle]::WaitTimeout) {
                    $failed = $waits[0].Part
                    $problem = "$(($waits | ForEach-Object { $_.Part }) -join ', ') neither answered nor ended within $Deadline s (-ReadyDeadline); the stack is stopped"
                    break
                }
                if ($signal -ge $listening.Count) {
                    $failed = $waits[$signal - $listening.Count].Part
                    $problem = "$failed ended before it answered; the stack is stopped"
                    break
                }
                $w = $listening[$signal]
                $w.Signalled = $true
                if ($w.Part -eq 'control' -and $brokersLeft -gt 0) { continue }
            }
            # The host has started: one request says whether it is this part that answers, and with 200.
            $wrong = Test-PartAnswer $layout $w.Part 30
            if ($wrong) {
                $failed = $w.Part
                $problem = "$wrong; the stack is stopped"
                break
            }
            Write-Host "  $($w.Part) answers ($([int]$watch.Elapsed.TotalSeconds) s)" -ForegroundColor DarkGray
            $w.Ready.Dispose()
            [void]$waits.Remove($w)
        }
    } finally {
        foreach ($w in $waits) { $w.Ready.Dispose() }
    }

    if ($problem) {
        Write-StackLog -Source 'stack' "offset ${at}: start failed: $problem"
        Stop-StackParts $at | Out-Null
        Write-Host "Start failed: $problem." -ForegroundColor Red
        Show-PartLogTail $at $failed
        exit 1
    }
    $running = @(Get-StackParts $at | Where-Object { $_.Process })
    Write-StackLog -Source 'stack' ("offset ${at}: started, {0}" -f (($running | Sort-Object { - (Get-PartRank $_.Part) } | ForEach-Object { "$($_.Part) pid=$($_.Process.Id)" }) -join ', '))
    Write-Host ''
    Write-Host "  Kafka bootstrap  $($layout.bootstrap)" -ForegroundColor White
    Write-Host "  Control UI       $($layout.controlUrl)" -ForegroundColor White
    Write-Host "  broker REST      $(($layout.brokers | ForEach-Object { $_.rest }) -join '  ')" -ForegroundColor DarkGray
    Write-Host ''
    $suffix = if ($at -ne 0) { " -Offset $at" } else { '' }
    Write-Host "Stop with ./scripts/stack.ps1 stop$suffix; the state is ./scripts/stack.ps1 status$suffix." -ForegroundColor DarkGray
}

# ── status ──────────────────────────────────────────────────────────────────────────────────────────────────

# stack.json keeps the time as ISO 8601, which ConvertFrom-Json reads back as a date.
function Format-StackTime($Value) { if ($Value -is [datetime]) { $Value.ToString('yyyy-MM-dd HH:mm:ss') } else { "$Value" } }

# What a broker says of itself: GET /health, with its Raft state and the leader it knows.
function Get-BrokerHealth([string] $Rest) {
    try {
        $r = Invoke-WebRequest -Uri "$Rest/health" -TimeoutSec 5 -SkipHttpErrorCheck
        $line = "GET /health $($r.StatusCode)"
        try {
            $h = $r.Content | ConvertFrom-Json
            $line += ", raft $($h.raft_state)$(if ($null -ne $h.raft_leader_id) { " (leader $($h.raft_leader_id))" }), $($h.brokers_count) broker(s) known, $($h.topics_count) topic(s)"
        } catch { }
        [pscustomobject]@{ Ok = $r.StatusCode -eq 200; Line = $line }
    } catch {
        [pscustomobject]@{ Ok = $false; Line = 'silent' }
    }
}

<#
    What runs of the stack at an offset, part by part, from the parts found (Get-StackParts) and its stack.json: each
    part's line for status, and the stack's condition: 'running' (every part runs and answers), 'stopped' (no part
    runs) or 'partial' (anything between, or a stack whose stack.json is gone).
#>
function Get-StackCondition([int] $At, [object[]] $Found) {
    $state = Read-StackState $At
    $parts = @($Found | Where-Object { $_.Offset -eq $At })
    $lines = [System.Collections.Generic.List[object]]::new()
    if ($parts.Count -eq 0) { return [pscustomobject]@{ Condition = 'stopped'; State = $state; Lines = $lines; Running = 0; Expected = 0 } }
    if (-not $state -or -not $state.brokers) { return [pscustomobject]@{ Condition = 'partial'; State = $null; Lines = $lines; Running = $parts.Count; Expected = 0 } }
    $answering = 0
    foreach ($b in $state.brokers) {
        $p = $parts | Where-Object { $_.Part -eq $b.name -and $_.Process } | Select-Object -First 1
        if (-not $p) { $lines.Add([pscustomobject]@{ Text = ('  {0,-8} not running' -f $b.name); Colour = 'Red' }); continue }
        $health = Get-BrokerHealth $b.rest
        if ($health.Ok) { $answering++ }
        $lines.Add([pscustomobject]@{
            Text   = ('  {0,-8} pid {1,-6} id {2}  Kafka {3}:{4}  REST {5}  replication {6}  {7}{8}' -f $b.name, $p.Process.Id, $b.id, $state.host, $b.kafka, $b.rest, $b.replication, $health.Line, $(if (-not $p.Console) { ' (its console gone; stop still ends it in order)' }))
            Colour = if ($health.Ok) { 'Gray' } else { 'Yellow' } })
    }
    $c = $parts | Where-Object { $_.Part -eq 'control' -and $_.Process } | Select-Object -First 1
    if (-not $c) { $lines.Add([pscustomobject]@{ Text = '  control  not running'; Colour = 'Red' }) }
    else {
        $answer = Test-PartAnswer $state 'control' 10
        if (-not $answer) { $answering++ }
        $lines.Add([pscustomobject]@{ Text = ('  {0,-8} pid {1,-6} {2}  {3}' -f 'control', $c.Process.Id, $state.controlUrl, $(if ($answer) { $answer } else { 'GET / 200' })); Colour = if ($answer) { 'Yellow' } else { 'Gray' } })
    }
    $expected = @($state.brokers).Count + 1
    $running = @($parts | Where-Object { $_.Process }).Count
    [pscustomobject]@{ Condition = $(if ($answering -eq $expected -and $running -eq $expected) { 'running' } else { 'partial' }); State = $state; Lines = $lines; Running = $running; Expected = $expected }
}

# Shows the stack at an offset, or every running stack of this checkout, and returns status's exit code: 0 every
# stack shown runs complete, 1 nothing runs, 2 something runs in part.
function Show-StackStatus([Nullable[int]] $Only) {
    $found = @(Get-StackParts $Only)
    $offsets = if ($null -ne $Only) { @($Only) } else { @($found | ForEach-Object { $_.Offset } | Sort-Object -Unique) }
    if ($offsets.Count -eq 0) { Write-Host 'No stack of this checkout is running.'; return 1 }
    $conditions = foreach ($at in $offsets) {
        $s = Get-StackCondition $at $found
        $s.Condition
        if ($s.Condition -eq 'stopped') {
            Write-Host "Stack at offset ${at}: not running."
            if ($s.State) { Write-Host "  last started $(Format-StackTime $s.State.startedAt) with $($s.State.brokerCount) broker(s)" -ForegroundColor DarkGray }
            continue
        }
        if (-not $s.State) {
            Write-Host "Stack at offset ${at}: $($s.Running) part(s) run, but its stack.json is gone; stop it and start it again." -ForegroundColor Yellow
            continue
        }
        $what = if ($s.Condition -eq 'running') { 'running' } else { 'running in part' }
        Write-Host "Stack at offset ${at}: $what, $($s.Running) of $($s.Expected) part(s), $($s.State.brokerCount) broker(s)" -ForegroundColor $(if ($s.Condition -eq 'running') { 'Green' } else { 'Yellow' })
        foreach ($line in $s.Lines) { Write-Host $line.Text -ForegroundColor $line.Colour }
        Write-Host "  Kafka bootstrap  $($s.State.bootstrap)"
        Write-Host "  Control UI       $($s.State.controlUrl)"
        Write-Host "  since $(Format-StackTime $s.State.startedAt)   state $([IO.Path]::GetRelativePath($Repo, (Join-Path (Get-StackDir $at) 'stack.json')))   logs $([IO.Path]::GetRelativePath($Repo, (Join-Path (Get-StackDir $at) 'logs')))" -ForegroundColor DarkGray
    }
    if (@($conditions | Where-Object { $_ -ne 'stopped' }).Count -eq 0) { return 1 }
    if (@($conditions | Where-Object { $_ -ne 'running' }).Count -gt 0) { return 2 }
    0
}
# ── the verbs ───────────────────────────────────────────────────────────────────────────────────────────────

$called = [ordered]@{}
foreach ($key in $PSBoundParameters.Keys) { if ($key -ne 'Verb') { $called[$key] = $PSBoundParameters[$key] } }
# status and log only read: they leave no line of their own.
if ($Verb -notin 'status', 'log') {
    Write-StackLog -Source 'stack' ("called: " + (Format-StackCall "stack.ps1 $Verb" $called))
    Write-StackVanished -Source 'stack'
}

# start, restart and stop of one stack take turns: a second start while the first still waits for its parts would
# find the ports taken by them, so it waits for the first to finish and then finds the stack running and uses it. A
# named mutex per stacks root and offset, a handshake and not a pause; Windows hands it on when its holder ends.
if ($Verb -in 'start', 'restart', 'stop') {
    $rootKey = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($StacksRoot.ToLowerInvariant()))).Substring(0, 16)
    $stackLock = [System.Threading.Mutex]::new($false, "Local\SurgewaveStack-$rootKey-$Offset")
    $taken = $false
    try { $taken = $stackLock.WaitOne(0) } catch [System.Threading.AbandonedMutexException] { $taken = $true }
    if (-not $taken) {
        Write-Host "Another start, restart or stop of the stack at offset $Offset is running; waiting for it to finish..." -ForegroundColor DarkGray
        Write-StackLog -Source 'stack' "offset ${Offset}: waiting for another start, restart or stop of the stack"
        try { [void]$stackLock.WaitOne() } catch [System.Threading.AbandonedMutexException] { }
    }
}

switch ($Verb) {
    'start' {
        # A stack that runs complete, with the brokers asked for, is used as it is: a second caller (Terracle's stack,
        # a person) gets the stack the first one started.
        $running = Get-StackCondition $Offset @(Get-StackParts $Offset)
        if ($running.Condition -eq 'running' -and [int]$running.State.brokerCount -eq $Brokers) {
            Write-StackLog -Source 'stack' "offset ${Offset}: runs already with $Brokers broker(s), every part answering; used as it is"
            Write-Host "The stack at offset $Offset runs already with $Brokers broker(s), every part answering; used as it is." -ForegroundColor Green
            if ($Build -or $NoBuild) { Write-Host '  -Build and -NoBuild do not apply to a stack in use; restart runs a new build.' -ForegroundColor DarkGray }
            Write-Host ''
            Write-Host "  Kafka bootstrap  $($running.State.bootstrap)" -ForegroundColor White
            Write-Host "  Control UI       $($running.State.controlUrl)" -ForegroundColor White
            Write-Host ''
            exit 0
        }
        if ($running.Condition -ne 'stopped') {
            $why = if ($running.Condition -eq 'running') { "with $($running.State.brokerCount) broker(s), not $Brokers" } else { "in part ($($running.Running) of $($running.Expected) part(s) running)" }
            Write-Host "The stack at offset $Offset runs $why; it is stopped and started afresh." -ForegroundColor Yellow
            Write-StackLog -Source 'stack' "offset ${Offset}: runs $why; stopping it to start afresh"
        }
        $state = [ordered]@{
            offset      = $Offset
            brokerCount = $Brokers
            startedAt   = $null
        }
        Stop-StackParts $Offset | Out-Null
        if ($script:StackLeftovers.Count -gt 0) { throw "Processes of the stack at offset $Offset outlived the stop; they are named above." }
        Update-StackBuild $Build $NoBuild
        Start-StackParts $state $true $ReadyDeadline
    }
    'restart' {
        $state = Read-StackState $Offset
        if (-not $state) { throw "No stack was started at offset $Offset; start it with ./scripts/stack.ps1 start$(if ($Offset -ne 0) { " -Offset $Offset" })." }
        Stop-StackParts $Offset | Out-Null
        if ($script:StackLeftovers.Count -gt 0) { throw "Processes of the stack at offset $Offset outlived the stop; they are named above." }
        # stack.json carries the number of brokers as start recorded it: the restart runs the same stack, on its data,
        # with the build it finds or makes.
        Update-StackBuild $Build $NoBuild
        Start-StackParts $state $false $ReadyDeadline
    }
    'stop' {
        $ran = Stop-StackParts $Offset -Hard:$Force
        $leftovers = $script:StackLeftovers.Count
        Write-StackLog -Source 'stack' "offset ${Offset}: $(if ($ran) { 'stopped' } else { 'nothing to stop' })"
        if ($leftovers -gt 0) {
            Write-StackLog -Source 'stack' "offset ${Offset}: $leftovers process(es) still run after the stop"
            Write-Host "Stopped, but $leftovers process(es) of the stack still run." -ForegroundColor Red
            exit 1
        }
        Write-Host 'Stopped.' -ForegroundColor Green
    }
    'status' {
        exit (Show-StackStatus $(if ($offsetNamed) { $Offset } else { $null }))
    }
    'log' {
        $file = if ($Part) { Get-PartLog $Offset $Part } else { $script:StackLogPath }
        if (-not (Test-Path $file)) { throw "No log yet: $file" }
        if ($Follow) { Get-Content -Path $file -Tail $Tail -Wait } else { Get-Content -Path $file -Tail $Tail }
    }
    'build' {
        Invoke-StackBuild
    }
}

exit 0
