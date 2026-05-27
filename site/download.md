---
layout: page
title: Download
subtitle: Every way to get Surgewave running.
description: Install Surgewave via dotnet tool, Docker, or from source. Guides and container images.
---

## CLI (dotnet tool)

```bash
dotnet tool install -g Kuestenlogik.Surgewave.Tool
surgewave broker start --port 9092
```

## NuGet packages

```xml
<PackageReference Include="Kuestenlogik.Surgewave.Client" />
<PackageReference Include="Kuestenlogik.Surgewave.Runtime" />
<PackageReference Include="Kuestenlogik.Surgewave.Streams" />
```

## Docker

```bash
docker run --rm -p 9092:9092 -p 9093:9093 -p 5050:5050 \
  kuestenlogik/surgewave-broker:latest
```

Default ports map straight through; override via environment variables or a
mounted `appsettings.json` &mdash; see
[Broker configuration](/docs/configuration/broker.html).

## From source

```bash
git clone https://github.com/Kuestenlogik/Surgewave
cd Surgewave
dotnet build Kuestenlogik.Surgewave.slnx -c Release
dotnet run --project src/Kuestenlogik.Surgewave.Broker
```

## Verifying downloads

Surgewave `.swpkg` plugin packages are signed. See the
[Plugin Signing guide](/docs/security/plugin-signing.html) for how to set up
a trust store and verify releases.
