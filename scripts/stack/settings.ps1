#requires -Version 7.5
# The blank line below keeps the help apart: a comment directly above <# joins the help block, and Get-Help then
# finds no help.

<#
.SYNOPSIS
    Internal to scripts/stack.ps1: the settings of a stack's parts, as its env/<part>.json files hold them.

.DESCRIPTION
    Dot-sourced by scripts/stack.ps1. A part's console clears every Surgewave__*, SURGEWAVE_*, Kestrel__* and
    Broker__* of its caller (scripts/stack/part.ps1), so what a part is to know reaches it here or not at all. The
    hosts read environment variables with __ for levels, the Kestrel endpoints included; everything not named here
    comes from the part's own appsettings.json.
#>

<#
.SYNOPSIS
    The environment of one broker of a stack: its own appsettings.json, and on top of it what makes this broker this
    broker.

.DESCRIPTION
    Its id, its ports (Kafka, gRPC and REST, replication, and every Kestrel endpoint of its configuration), its data
    directory under the stack's folder, and the plugins of its own copy of the build only, so a plugin installed for
    this user or this machine never replaces the Kafka plugin the checkout built. A broker of a cluster also learns
    the cluster: every broker as id:host:port:replicationPort (Surgewave:ClusterNodes) and the voters of the
    controller quorum as id@host:replicationPort (Surgewave:ControllerQuorumVoters, #172). A single broker gets
    neither and runs standalone.

.PARAMETER Layout
    The stack's layout as stack.json holds it: host, brokers (each with id, ports and endpoints), control.

.PARAMETER Broker
    The broker's entry in the layout.

.PARAMETER DataDirectory
    The broker's data directory.

.PARAMETER PluginsDirectory
    The plugins folder of the broker's copy of the build.
#>
function New-StackBrokerSettings {
    param(
        [Parameter(Mandatory)] [System.Collections.IDictionary] $Layout,
        [Parameter(Mandatory)] [System.Collections.IDictionary] $Broker,
        [Parameter(Mandatory)] [string] $DataDirectory,
        [Parameter(Mandatory)] [string] $PluginsDirectory
    )
    $settings = [ordered]@{
        'ASPNETCORE_ENVIRONMENT'         = 'Development'
        # The folder of its appsettings.json, whatever the working directory: a host that finds no Kestrel endpoints
        # opens ASP.NET Core's default http://localhost:5000, a port no offset moves.
        'ASPNETCORE_CONTENTROOT'         = Split-Path -Parent $PluginsDirectory
        'Surgewave__BrokerId'            = [string]$Broker.id
        'Surgewave__Port'                = [string]$Broker.kafka
        'Surgewave__GrpcPort'            = [string]$Broker.grpc
        'Surgewave__ReplicationPort'     = [string]$Broker.replication
        'Surgewave__DataDirectory'       = $DataDirectory
        'Surgewave__LogDirectory'        = Join-Path $DataDirectory 'logs'
        'Surgewave__PluginsDirectory'    = $PluginsDirectory
    }
    # The PostgreSQL wire listener, when the broker's configuration switches it on.
    if ($Broker.postgreSql) { $settings['Surgewave__PostgreSql__Port'] = [string]$Broker.postgreSql }
    foreach ($endpoint in $Broker.endpoints.GetEnumerator()) {
        $settings["Kestrel__Endpoints__$($endpoint.Key)__Url"] = $endpoint.Value
    }
    $brokers = @($Layout.brokers)
    if ($brokers.Count -gt 1) {
        $settings['Surgewave__ClusterNodes'] = ($brokers | ForEach-Object { '{0}:{1}:{2}:{3}' -f $_.id, $Layout.host, $_.kafka, $_.replication }) -join ','
        $settings['Surgewave__ControllerQuorumVoters'] = ($brokers | ForEach-Object { '{0}@{1}:{2}' -f $_.id, $Layout.host, $_.replication }) -join ','
    }
    $settings
}

<#
.SYNOPSIS
    The environment of a stack's Control UI: its own port, and broker 1 of the stack as the broker it talks to.

.DESCRIPTION
    The UI's HTTP endpoint moved by the stack's offset; broker 1's REST endpoint as its API (Broker:ApiUrl, the
    gRPC/REST port, https while the broker's Surgewave:GrpcUseTls is on); broker 1's host and ports for the native
    client of its marketplace service; and the stores it keeps (roles, alerts) under the stack's folder, so they
    outlive the copy of the build it runs from.

.PARAMETER Layout
    The stack's layout as stack.json holds it.

.PARAMETER DataDirectory
    The Control UI's data directory.

.PARAMETER ContentRoot
    The Control UI's copy of the build, where its appsettings.json lies.
#>
function New-StackControlSettings {
    param(
        [Parameter(Mandatory)] [System.Collections.IDictionary] $Layout,
        [Parameter(Mandatory)] [string] $DataDirectory,
        [Parameter(Mandatory)] [string] $ContentRoot
    )
    $first = @($Layout.brokers)[0]
    [ordered]@{
        'ASPNETCORE_ENVIRONMENT'          = 'Development'
        'ASPNETCORE_CONTENTROOT'          = $ContentRoot
        'Kestrel__Endpoints__Http__Url'   = $Layout.control.endpoint
        'Broker__ApiUrl'                  = $first.rest
        'Surgewave__Host'                 = $Layout.host
        'Surgewave__Port'                 = [string]$first.kafka
        'Surgewave__GrpcPort'             = [string]$first.grpc
        'Surgewave__Roles__StorePath'     = Join-Path $DataDirectory 'roles.json'
        'Surgewave__Alerting__StorePath'  = Join-Path $DataDirectory 'alerts.json'
    }
}
