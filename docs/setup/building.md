# Build, Publish & Run

This guide walks you through every step from cloning the repository to a running broker — in all supported deployment variants.

## Prerequisites

| Tool | Minimum version | Required for |
|------|-----------------|-------------|
| [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0) | 10.0 | All variants |
| [Git](https://git-scm.com/) | any | Cloning |
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | any | Container variant |
| [PowerShell](https://github.com/PowerShell/PowerShell) | 7+ | Scripts (`scripts/*.ps1`) |

---

## Step 1 — Clone

```bash
git clone https://github.com/Kuestenlogik/Surgewave.git
cd Surgewave
```

---

## Step 2 — Build

The build compiles the full solution and produces NuGet packages under `artifacts/packages/`.

**Using the script (recommended):**

```powershell
.\scripts\build.ps1
```

**Or directly with the .NET CLI:**

```bash
dotnet build Kuestenlogik.Surgewave.slnx -c Release
```

> `publish.ps1` (step 4) performs its own implicit build, so you only need this step when you specifically want the `.nupkg` artifacts — for example to feed the `surgewave-local` NuGet feed consumed by the Samples solution.

---

## Step 3 — Test (optional)

```bash
dotnet test Kuestenlogik.Surgewave.slnx -v normal
```

---

## Step 4 — Choose a deployment variant

Pick the variant that matches your use case.

---

### Variant A — Development (`dotnet run`)

Fastest way to get started. No publish step required, uses the Debug/Release build output directly.

```bash
# Broker only
dotnet run --project src/Kuestenlogik.Surgewave.Broker

# Broker + Control UI
dotnet run --project src/Kuestenlogik.Surgewave.Broker
dotnet run --project src/Kuestenlogik.Surgewave.Control --urls "http://localhost:5050"
```

**Endpoints:**

| Service | URL / Address |
|---------|--------------|
| Kafka protocol | `localhost:9092` |
| gRPC | `localhost:9093` |
| Control UI | `http://localhost:5050` |
| Gateway HTTP | `http://localhost:8082` |

> Suitable for development and local testing. Restart required on code changes.

---

### Variant B — Published Executables

Produces self-contained, trimmed, single-file binaries for the target platform. No .NET runtime needed on the target machine.

**Publish:**

```powershell
# All services for the current platform (default: win-x64)
.\scripts\publish.ps1

# Specific platform
.\scripts\publish.ps1 -Runtime linux-x64

# Specific services only
.\scripts\publish.ps1 -Service Broker,Control
```

Output: `artifacts/pub/<Service>/<Runtime>/`

| Service | Executable |
|---------|-----------|
| Broker | `surgewave-broker` / `surgewave-broker.exe` |
| Gateway | `surgewave-gateway` / `surgewave-gateway.exe` |
| Control UI | `surgewave-control` / `surgewave-control.exe` |
| Marketplace | `surgewave-marketplace` / `surgewave-marketplace.exe` |
| Connector | `surgewave-connect` / `surgewave-connect.exe` |
| CLI | `surgewave` / `surgewave.exe` |

**Run (Windows — via script):**

```powershell
# Starts Broker, Gateway and Control in separate windows
.\scripts\start.ps1

# With PostgreSQL wire protocol (port 5432, for materialized-view demos)
# Only use when port 5432 is free — a local PostgreSQL server would conflict
.\scripts\start.ps1 -PostgreSql

# Stop everything
.\scripts\stop.ps1
```

**Run (manually / Linux):**

```bash
# Broker
./artifacts/pub/Broker/linux-x64/surgewave-broker

# Control UI on a custom port
./artifacts/pub/Control/linux-x64/surgewave-control --urls http://localhost:5050

# Gateway
./artifacts/pub/Gateway/linux-x64/surgewave-gateway
```

**Run as a systemd service (Linux):**

```ini
# /etc/systemd/system/surgewave-broker.service
[Unit]
Description=Surgewave Broker
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/surgewave/broker
ExecStart=/opt/surgewave/broker/surgewave-broker
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now surgewave-broker
sudo systemctl status surgewave-broker
```

**Run as a Windows Service:**

```powershell
sc.exe create SurgewaveBroker `
    binPath= "C:\surgewave\broker\surgewave-broker.exe" `
    start= auto
sc.exe start SurgewaveBroker
```

---

### Variant C — Container (Docker)

Produces OCI-compliant container images using .NET's native container support — no Dockerfile required.

**Publish container images:**

```powershell
# Containers only (all services)
.\scripts\publish.ps1 -Mode Container

# Executables AND containers (default)
.\scripts\publish.ps1

# One service only
.\scripts\publish.ps1 -Mode Container -Service Broker
```

**Every run produces portable `.tar` archives under `artifacts/pub/containers/`** —
one per service. These are the real deployment artifacts and can be moved to
any host that has Docker or Podman installed.

```
artifacts/pub/containers/
├── broker.tar
├── gateway.tar
├── control.tar
├── marketplace.tar
└── connector.tar
```

**With Docker running during publish** — each tar is additionally loaded into
the local Docker daemon right after it is produced, so you can `docker run`
immediately without an extra step:

```bash
docker images surgewave/
# REPOSITORY          TAG     IMAGE ID   ...
# surgewave/surgewave-broker  0.1.0   ...
# surgewave/surgewave-gateway 0.1.0   ...
# surgewave/surgewave-control 0.1.0   ...
# ...
```

**Loading `.tar` archives into Docker later (or on another host):**

Use `docker load -i <file>` to register a tar with the local Docker daemon.
The tag is embedded in the archive, so no extra metadata is required.

```bash
# Single tar
docker load -i artifacts/pub/containers/broker.tar
# Loaded image: surgewave/surgewave-broker:0.1.0

# All tars at once (bash)
for tar in artifacts/pub/containers/*.tar; do
  docker load -i "$tar"
done

# All tars at once (PowerShell)
Get-ChildItem artifacts/pub/containers/*.tar | ForEach-Object { docker load -i $_ }

# Verify what got registered
docker images surgewave/
```

Podman uses the same syntax with `podman load -i <file>`. The archives are
portable — copy them to any host (`scp`, USB stick, artifact registry) and
load them there with the same command.

**Run with Docker:**

```bash
# Broker (minimal)
docker run -d \
  --name surgewave-broker \
  -p 9092:9092 \
  -p 9093:9093 \
  -v surgewave-data:/app/data \
  surgewave/surgewave-broker:0.1.0

# Control UI
docker run -d \
  --name surgewave-control \
  -p 5050:5050 \
  surgewave/surgewave-control:0.1.0

# Gateway
docker run -d \
  --name surgewave-gateway \
  -p 8082:8082 \
  surgewave/surgewave-gateway:0.1.0
```

**Run the CLI in Docker (one-shot):**

The CLI ships as `surgewave/surgewave-cli:0.1.0` — the same image referenced by
`deployments/docker/docker-compose.yml` under the `cli` profile. Because
the CLI is not a long-running service it is excluded from `docker compose up`
and invoked on demand via `docker compose run` or plain `docker run`:

```bash
# From the compose directory — uses the compose network automatically
cd deployments/docker
docker compose run --rm surgewave-cli --help
docker compose run --rm surgewave-cli topics list --broker surgewave-broker:9092
docker compose run --rm surgewave-cli plugin list --broker http://surgewave-broker:9093
```

```bash
# Or with plain docker run — attach to the compose network manually
docker run --rm -it --network docker_surgewave-network surgewave/surgewave-cli:0.1.0 --help
docker run --rm -it --network docker_surgewave-network surgewave/surgewave-cli:0.1.0 \
  topics list --broker surgewave-broker:9092
```

Convenient shell alias so `surgewave <cmd>` always hits the containerised CLI:

```bash
# bash / zsh
alias surgewave='docker compose -f deployments/docker/docker-compose.yml run --rm surgewave-cli'

# PowerShell
function surgewave { docker compose -f deployments/docker/docker-compose.yml run --rm surgewave-cli @args }
```

**Run with Docker Compose:**

```yaml
# docker-compose.yml
services:
  broker:
    image: surgewave/surgewave-broker:0.1.0
    ports:
      - "9092:9092"   # Kafka protocol
      - "9093:9093"   # gRPC
    environment:
      - Surgewave__Storage__DataDirectory=/app/data
      - Surgewave__Storage__LogDirectory=/app/logs
      - DOTNET_RUNNING_IN_CONTAINER=true
    volumes:
      - surgewave-data:/app/data
      - surgewave-logs:/app/logs
    restart: unless-stopped

  control:
    image: surgewave/surgewave-control:0.1.0
    ports:
      - "5050:5050"
    restart: unless-stopped

  gateway:
    image: surgewave/surgewave-gateway:0.1.0
    ports:
      - "8082:8082"
    restart: unless-stopped

volumes:
  surgewave-data:
  surgewave-logs:
```

```bash
docker compose up -d
docker compose logs -f broker
```

---

## Step 5 — Verify

Once any variant is running:

```bash
# List topics (Surgewave CLI)
surgewave topics list

# Or with a Kafka client
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Control UI
open http://localhost:5050

# Health endpoint
curl https://localhost:9093/health
```

---

## Ports Reference

| Port | Protocol | Description |
|------|----------|-------------|
| `9092` | TCP | Kafka wire protocol |
| `9093` | TCP | gRPC API |
| `9091` | TCP | Native Surgewave protocol |
| `1883` | TCP | MQTT |
| `5432` | TCP | PostgreSQL wire protocol (opt-in) |
| `5050` | HTTP | Control UI |
| `8082` | HTTP | Gateway / REST proxy |

---

## Script Reference

| Script | Purpose |
|--------|---------|
| `scripts/build.ps1` | Compile and produce NuGet packages to `artifacts/packages/` |
| `scripts/publish.ps1` | Publish executables and/or container images |
| `scripts/start.ps1` | Launch published Broker, Gateway and Control in separate windows |
| `scripts/stop.ps1` | Stop services started by `start.ps1` |
| `scripts/docs.ps1` | Build DocFX docs (`-Serve` for local preview at `http://localhost:8080`) |
| `scripts/run-coverage.ps1` | Tests with code coverage report |
| `scripts/run-integration-tests.ps1` | Start broker, run Kafka compatibility tests, stop broker |
| `scripts/run-all-benchmarks.ps1` | Run all BenchmarkDotNet benchmark suites |

---

## Next Steps

- [Configuration](configuration.md) — all `appsettings.json` options and environment variables
- [Plugin System](../features/plugin-development.md) — install `.swpkg` plugins
- [Clustering](../clustering/index.md) — multi-broker setup
- [Kubernetes Deployment](../deployment/kubernetes.md) — production orchestration
