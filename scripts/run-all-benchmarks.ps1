# Surgewave Benchmark Suite - Automated Runner
# Runs all benchmark variants and updates the baseline JSON file

param(
    [int]$MessageCount = 1000000,
    [int]$MessageSize = 100,
    [int]$BatchSize = 1000,
    [string]$KafkaBootstrap = "localhost:29092",
    [string]$RedpandaBootstrap = "localhost:19092",
    [switch]$SkipKafka,
    [switch]$SkipRedpanda,
    [switch]$UpdateBaseline,
    [switch]$Verbose
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
$baselineFile = Join-Path $projectRoot "benchmarks" "baselines" "benchmark-baseline.json"
$benchmarkProject = Join-Path $projectRoot "benchmarks" "Kuestenlogik.Surgewave.Benchmarks"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Surgewave Benchmark Suite - Automated" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Configuration:" -ForegroundColor Yellow
Write-Host "  Messages:           $MessageCount"
Write-Host "  Message Size:       $MessageSize bytes"
Write-Host "  Batch Size:         $BatchSize"
Write-Host "  Kafka Bootstrap:    $KafkaBootstrap"
Write-Host "  Redpanda Bootstrap: $RedpandaBootstrap"
Write-Host "  Skip Kafka:         $SkipKafka"
Write-Host "  Skip Redpanda:      $SkipRedpanda"
Write-Host "  Update Baseline:    $UpdateBaseline"
Write-Host ""

# Build the benchmark project
Write-Host "Building benchmark project..." -ForegroundColor Yellow
Push-Location $projectRoot
try {
    dotnet build -c Release benchmarks/Kuestenlogik.Surgewave.Benchmarks --verbosity quiet
    if ($LASTEXITCODE -ne 0) {
        throw "Build failed"
    }
    Write-Host "Build successful" -ForegroundColor Green
} finally {
    Pop-Location
}

# Results storage
$results = @{
    version = "2.0"
    date = (Get-Date).ToString("yyyy-MM-dd")
    environment = @{
        os = [System.Environment]::OSVersion.Platform.ToString()
        runtime = [System.Runtime.InteropServices.RuntimeInformation]::FrameworkDescription
        cpu = [System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture.ToString()
    }
}

function Parse-BenchmarkOutput {
    param([string]$Output, [string]$Pattern)

    if ($Output -match $Pattern) {
        return [double]($Matches[1] -replace '[,\.]', '' -replace '(\d+)(\d{3})$', '$1.$2')
    }
    return $null
}

function Run-Benchmark {
    param(
        [string]$Name,
        [string[]]$BenchArgs
    )

    Write-Host ""
    Write-Host "Running: $Name" -ForegroundColor Cyan
    Write-Host "Command: dotnet run -- $($BenchArgs -join ' ')" -ForegroundColor DarkGray

    Push-Location $projectRoot
    try {
        $argList = @("run", "--project", "benchmarks/Kuestenlogik.Surgewave.Benchmarks", "-c", "Release", "--no-build", "--") + $BenchArgs
        $output = & dotnet @argList 2>&1 | Out-String
        if ($Verbose) {
            Write-Host $output -ForegroundColor DarkGray
        }
        return $output
    } finally {
        Pop-Location
    }
}

# 1. Run Embedded Native Protocol benchmark
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  [1/4] Embedded Native Protocol" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$embeddedOutput = Run-Benchmark -Name "Embedded Native" -BenchArgs @("embedded", "$MessageCount", "$MessageSize", "$BatchSize")

# Parse embedded results (format: "123.456 msg/sec")
$nativeProducerMsgSec = 0
$nativeProducerMBSec = 0
$nativeConsumerMsgSec = 0
$nativeConsumerMBSec = 0

if ($embeddedOutput -match "Producer:\s+Time:\s+(\d+)\s+ms") {
    $produceMs = [int]$Matches[1]
    $nativeProducerMsgSec = [math]::Round($MessageCount * 1000 / $produceMs)
    $nativeProducerMBSec = [math]::Round($MessageCount * $MessageSize / 1024 / 1024 * 1000 / $produceMs, 1)
}
if ($embeddedOutput -match "Consumer:\s+Time:\s+(\d+)\s+ms") {
    $consumeMs = [int]$Matches[1]
    $nativeConsumerMsgSec = [math]::Round($MessageCount * 1000 / $consumeMs)
    $nativeConsumerMBSec = [math]::Round($MessageCount * $MessageSize / 1024 / 1024 * 1000 / $consumeMs, 1)
}

# Alternative parsing for throughput format
if ($embeddedOutput -match "Producer:.*?(\d[\d,\.]*)\s+msg/sec.*?(\d+[\.,]\d+)\s+MB/sec") {
    $nativeProducerMsgSec = [int]($Matches[1] -replace '[,\.]', '')
    $nativeProducerMBSec = [double]($Matches[2] -replace ',', '.')
}
if ($embeddedOutput -match "Consumer:.*?(\d[\d,\.]*)\s+msg/sec.*?(\d+[\.,]\d+)\s+MB/sec") {
    $nativeConsumerMsgSec = [int]($Matches[1] -replace '[,\.]', '')
    $nativeConsumerMBSec = [double]($Matches[2] -replace ',', '.')
}

Write-Host "  Native Producer: $nativeProducerMsgSec msg/sec ($nativeProducerMBSec MB/sec)" -ForegroundColor Green
Write-Host "  Native Consumer: $nativeConsumerMsgSec msg/sec ($nativeConsumerMBSec MB/sec)" -ForegroundColor Green

# 2. Run Embedded Protocol Comparison (Native vs Kafka protocol on same broker)
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  [2/4] Protocol Comparison (Embedded)" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$compareOutput = Run-Benchmark -Name "Embedded Compare" -BenchArgs @("embedded-compare", "$MessageCount", "$MessageSize", "$BatchSize")

$kafkaProducerMsgSec = 0
$kafkaProducerMBSec = 0
$kafkaConsumerMsgSec = 0
$kafkaConsumerMBSec = 0

# Parse Kafka protocol results from comparison output
# Output format:
#   KAFKA WIRE PROTOCOL (Confluent.Kafka client)
#   [Batched Producer Test]
#     Throughput: 1.086.957 msg/sec (103,7 MB/sec)
#   [Consumer Test]
#     Throughput: 1.408.451 msg/sec (134,3 MB/sec)
$lines = $compareOutput -split "`n"
$inKafkaSection = $false
$isProducerSection = $false
$isConsumerSection = $false
foreach ($line in $lines) {
    if ($line -match "KAFKA WIRE PROTOCOL") {
        $inKafkaSection = $true
        $isProducerSection = $false
        $isConsumerSection = $false
    }
    if ($inKafkaSection) {
        if ($line -match "\[.*Producer.*\]") {
            $isProducerSection = $true
            $isConsumerSection = $false
        }
        if ($line -match "\[.*Consumer.*\]") {
            $isProducerSection = $false
            $isConsumerSection = $true
        }
        if ($line -match "Throughput:\s+(\d[\d,\.]*)\s+msg/sec.*?\((\d+[\.,]\d+)\s+MB/sec") {
            $msgSec = [int]($Matches[1] -replace '[,\.]', '')
            $mbSec = [double]($Matches[2] -replace ',', '.')
            if ($isProducerSection -and $kafkaProducerMsgSec -eq 0) {
                $kafkaProducerMsgSec = $msgSec
                $kafkaProducerMBSec = $mbSec
            } elseif ($isConsumerSection -and $kafkaConsumerMsgSec -eq 0) {
                $kafkaConsumerMsgSec = $msgSec
                $kafkaConsumerMBSec = $mbSec
            }
        }
    }
}

Write-Host "  Kafka Producer:  $kafkaProducerMsgSec msg/sec ($kafkaProducerMBSec MB/sec)" -ForegroundColor Yellow
Write-Host "  Kafka Consumer:  $kafkaConsumerMsgSec msg/sec ($kafkaConsumerMBSec MB/sec)" -ForegroundColor Yellow

# 3. Four-way comparison (if Kafka and/or Redpanda Docker available)
if (-not $SkipKafka -or -not $SkipRedpanda) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  [3/5] Four-Way Comparison" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan

    # Check if Kafka is available
    $kafkaAvailable = $false
    if (-not $SkipKafka) {
        try {
            $tcpClient = New-Object System.Net.Sockets.TcpClient
            $host_port = $KafkaBootstrap -split ":"
            $tcpClient.Connect($host_port[0], [int]$host_port[1])
            $kafkaAvailable = $true
            $tcpClient.Close()
            Write-Host "  Kafka broker available at $KafkaBootstrap" -ForegroundColor Green
        } catch {
            Write-Host "  Kafka broker not available at $KafkaBootstrap - skipping" -ForegroundColor Yellow
        }
    }

    # Check if Redpanda is available
    $redpandaAvailable = $false
    if (-not $SkipRedpanda) {
        try {
            $tcpClient = New-Object System.Net.Sockets.TcpClient
            $host_port = $RedpandaBootstrap -split ":"
            $tcpClient.Connect($host_port[0], [int]$host_port[1])
            $redpandaAvailable = $true
            $tcpClient.Close()
            Write-Host "  Redpanda broker available at $RedpandaBootstrap" -ForegroundColor Green
        } catch {
            Write-Host "  Redpanda broker not available at $RedpandaBootstrap - skipping" -ForegroundColor Yellow
        }
    }

    if ($kafkaAvailable -or $redpandaAvailable) {
        $fourWayOutput = Run-Benchmark -Name "Four-Way" -BenchArgs @("four-way", "$MessageCount", "$MessageSize", $KafkaBootstrap, $RedpandaBootstrap)

        # Parse pure Kafka results
        if ($fourWayOutput -match "PURE KAFKA.*?Producer:\s+(\d[\d,\.]*)\s+msg/sec.*?(\d+[\.,]\d+)\s+MB/sec") {
            $results.pureKafka = @{
                producerMsgPerSec = [int]($Matches[1] -replace '[,\.]', '')
                producerMBPerSec = [double]($Matches[2] -replace ',', '.')
            }
        }
        if ($fourWayOutput -match "PURE KAFKA.*?Consumer:\s+(\d[\d,\.]*)\s+msg/sec.*?(\d+[\.,]\d+)\s+MB/sec") {
            if ($results.pureKafka) {
                $results.pureKafka.consumerMsgPerSec = [int]($Matches[1] -replace '[,\.]', '')
                $results.pureKafka.consumerMBPerSec = [double]($Matches[2] -replace ',', '.')
            }
        }

        # Parse Redpanda results
        if ($fourWayOutput -match "REDPANDA.*?Producer:\s+(\d[\d,\.]*)\s+msg/sec.*?(\d+[\.,]\d+)\s+MB/sec") {
            $results.redpanda = @{
                producerMsgPerSec = [int]($Matches[1] -replace '[,\.]', '')
                producerMBPerSec = [double]($Matches[2] -replace ',', '.')
            }
        }
        if ($fourWayOutput -match "REDPANDA.*?Consumer:\s+(\d[\d,\.]*)\s+msg/sec.*?(\d+[\.,]\d+)\s+MB/sec") {
            if ($results.redpanda) {
                $results.redpanda.consumerMsgPerSec = [int]($Matches[1] -replace '[,\.]', '')
                $results.redpanda.consumerMBPerSec = [double]($Matches[2] -replace ',', '.')
            }
        }
    }
} else {
    Write-Host ""
    Write-Host "  [3/5] Skipping external broker comparison (--SkipKafka --SkipRedpanda)" -ForegroundColor Yellow
}

# 4. BenchmarkDotNet micro-benchmarks (optional, takes longer)
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  [4/4] Serialization Benchmarks" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  (Skipped - run manually with: dotnet run -- --filter '*Serialization*')" -ForegroundColor DarkGray

# Build final results structure
$results.surgewaveNativeProtocol = @{
    description = "Surgewave Native Protocol (binary, optimized for Surgewave broker)"
    embedded = @{
        description = "EmbeddedSurgewave throughput ($MessageCount messages, $MessageSize bytes, batch $BatchSize)"
        client = "SurgewaveNativeClient + SurgewaveBatchingProducer"
        producer = @{
            messagesPerSec = $nativeProducerMsgSec
            mbPerSec = $nativeProducerMBSec
        }
        consumer = @{
            messagesPerSec = $nativeConsumerMsgSec
            mbPerSec = $nativeConsumerMBSec
        }
    }
}

$results.kafkaProtocol = @{
    description = "Kafka Wire Protocol (compatible with Apache Kafka clients)"
    embedded = @{
        description = "EmbeddedSurgewave throughput ($MessageCount messages, $MessageSize bytes, batch $BatchSize)"
        client = "Confluent.Kafka producer/consumer"
        producer = @{
            messagesPerSec = $kafkaProducerMsgSec
            mbPerSec = $kafkaProducerMBSec
        }
        consumer = @{
            messagesPerSec = $kafkaConsumerMsgSec
            mbPerSec = $kafkaConsumerMBSec
        }
    }
}

# Calculate protocol comparison
if ($kafkaProducerMsgSec -gt 0 -and $kafkaConsumerMsgSec -gt 0) {
    $prodAdvantage = [math]::Round(($nativeProducerMsgSec - $kafkaProducerMsgSec) / $kafkaProducerMsgSec * 100, 1)
    $consAdvantage = [math]::Round(($nativeConsumerMsgSec - $kafkaConsumerMsgSec) / $kafkaConsumerMsgSec * 100, 1)

    $results.protocolComparison = @{
        description = "Surgewave Native vs Kafka Wire Protocol on same embedded broker ($MessageCount messages, $MessageSize bytes)"
        producer = @{
            nativeMsgPerSec = $nativeProducerMsgSec
            kafkaMsgPerSec = $kafkaProducerMsgSec
            nativeAdvantage = "+$prodAdvantage%"
        }
        consumer = @{
            nativeMsgPerSec = $nativeConsumerMsgSec
            kafkaMsgPerSec = $kafkaConsumerMsgSec
            nativeAdvantage = "+$consAdvantage%"
        }
        command = "dotnet run -- embedded-compare $MessageCount $MessageSize $BatchSize"
    }
}

$results.commands = @{
    embeddedNative = "dotnet run -- embedded $MessageCount $MessageSize $BatchSize"
    embeddedCompare = "dotnet run -- embedded-compare $MessageCount $MessageSize $BatchSize"
    threeWay = "dotnet run -- three-way $MessageCount $MessageSize $KafkaBootstrap"
    fourWay = "dotnet run -- four-way $MessageCount $MessageSize $KafkaBootstrap $RedpandaBootstrap"
    serialization = "dotnet run -- --filter '*Serialization*'"
    simd = "dotnet run -- --filter '*SimdBigEndian*'"
    compression = "dotnet run -- --filter '*Compression*'"
}

# Print summary
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  BENCHMARK SUMMARY" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Protocol Comparison (on embedded Surgewave broker):" -ForegroundColor Yellow
Write-Host "  Native Producer:  $nativeProducerMsgSec msg/sec ($nativeProducerMBSec MB/sec)"
Write-Host "  Kafka Producer:   $kafkaProducerMsgSec msg/sec ($kafkaProducerMBSec MB/sec)"
Write-Host "  Native Consumer:  $nativeConsumerMsgSec msg/sec ($nativeConsumerMBSec MB/sec)"
Write-Host "  Kafka Consumer:   $kafkaConsumerMsgSec msg/sec ($kafkaConsumerMBSec MB/sec)"

if ($kafkaProducerMsgSec -gt 0) {
    Write-Host ""
    Write-Host "Native Protocol Advantage:" -ForegroundColor Green
    Write-Host "  Producer: +$prodAdvantage% faster than Kafka protocol"
    Write-Host "  Consumer: +$consAdvantage% faster than Kafka protocol"
}

# Update baseline file if requested
if ($UpdateBaseline) {
    Write-Host ""
    Write-Host "Updating baseline file: $baselineFile" -ForegroundColor Yellow
    $results | ConvertTo-Json -Depth 10 | Set-Content -Path $baselineFile -Encoding UTF8
    Write-Host "Baseline updated successfully" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "Run with -UpdateBaseline to save results to: $baselineFile" -ForegroundColor DarkGray
}

Write-Host ""
Write-Host "Done!" -ForegroundColor Green
