# Standalone Broker Setup

Run Surgewave as a standalone broker for development or single-node production.

## Prerequisites

- .NET 10 SDK
- Git

## Installation

### From Source

```bash
# Clone repository
git clone https://github.com/Kuestenlogik/Surgewave.git
cd Surgewave

# Build
dotnet build -c Release

# Run broker
cd src/Kuestenlogik.Surgewave.Broker
dotnet run -c Release
```

### Build CLI Tool

```bash
# Build and publish CLI
cd src/Kuestenlogik.Surgewave.Cli
dotnet publish -c Release -o ~/.surgewave/bin

# Add to PATH
export PATH="$PATH:~/.surgewave/bin"

# Verify installation
surgewave --version
```

## Configuration

Create or modify `appsettings.json`:

```json
{
  "Surgewave": {
    "BrokerId": 1,
    "Host": "0.0.0.0",
    "Port": 9092,
    "GrpcPort": 9093,
    "DataDirectory": "./data",
    "LogDirectory": "./logs",
    "StorageMode": "File",
    "AutoCreateTopics": true,
    "DefaultNumPartitions": 3,
    "DefaultReplicationFactor": 1
  }
}
```

## Running as a Service

### Linux (systemd)

Create `/etc/systemd/system/surgewave.service`:

```ini
[Unit]
Description=Surgewave Message Broker
After=network.target

[Service]
Type=simple
User=surgewave
WorkingDirectory=/opt/surgewave
ExecStart=/usr/bin/dotnet /opt/surgewave/Kuestenlogik.Surgewave.Broker.dll
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable surgewave
sudo systemctl start surgewave
sudo systemctl status surgewave
```

### Windows (Service)

```powershell
# Install as Windows service
sc.exe create Surgewave binPath= "dotnet C:\Surgewave\Kuestenlogik.Surgewave.Broker.dll" start= auto
sc.exe start Surgewave
```

## Verify Installation

```bash
# Check broker health
surgewave health

# List topics (should be empty initially)
surgewave topics list

# Create test topic
surgewave topics create test-topic

# Produce test message
surgewave produce test-topic --value "Hello, Surgewave!"

# Consume test message
surgewave consume test-topic --offset earliest
```

## Directory Structure

```
/opt/surgewave/                    # Installation directory
├── Kuestenlogik.Surgewave.Broker.dll       # Main broker assembly
├── appsettings.json          # Configuration
├── data/                     # Data directory
│   ├── topics/               # Topic data
│   └── __consumer_offsets/   # Consumer group offsets
└── logs/                     # Log files
```

## Environment Variables

Override configuration with environment variables:

```bash
export Surgewave__Port=9092
export Surgewave__DataDirectory=/data/surgewave
export Surgewave__StorageMode=File
```

## Next Steps

- [Configuration Reference](configuration.md) - All configuration options
- [Clustering](../clustering/index.md) - Multi-broker setup
- [Security](../security/index.md) - Enable authentication
