Name:           surgewave
Version:        0.1.0
Release:        1%{?dist}
Summary:        Surgewave - High-performance event streaming platform
License:        Apache-2.0
URL:            https://github.com/Kuestenlogik/Surgewave
BuildArch:      x86_64

%description
A drop-in Kafka replacement built with .NET 10.
Includes broker, CLI, and Control UI.
Features: Kafka 4.0 protocol, native high-performance protocol,
visual pipeline editor, AI integration, schema registry,
113 connectors, 10 storage engines.

%install
mkdir -p %{buildroot}/opt/surgewave/broker
mkdir -p %{buildroot}/opt/surgewave/cli
mkdir -p %{buildroot}/opt/surgewave/control
mkdir -p %{buildroot}/opt/surgewave/plugins
mkdir -p %{buildroot}/opt/surgewave/models
mkdir -p %{buildroot}/opt/surgewave/wasm-plugins
mkdir -p %{buildroot}/etc/surgewave
mkdir -p %{buildroot}/var/lib/surgewave/data
mkdir -p %{buildroot}/var/log/surgewave
mkdir -p %{buildroot}/usr/lib/systemd/system
mkdir -p %{buildroot}/usr/bin

# Copy binaries (from SOURCES)
cp -r %{_sourcedir}/broker/* %{buildroot}/opt/surgewave/broker/
cp -r %{_sourcedir}/cli/* %{buildroot}/opt/surgewave/cli/ 2>/dev/null || true
cp -r %{_sourcedir}/control/* %{buildroot}/opt/surgewave/control/ 2>/dev/null || true

# Config
cp %{_sourcedir}/appsettings.json %{buildroot}/etc/surgewave/
cp %{_sourcedir}/appsettings.Production.json %{buildroot}/etc/surgewave/

# Systemd units
cp %{_sourcedir}/surgewave-broker.service %{buildroot}/usr/lib/systemd/system/
cp %{_sourcedir}/surgewave-control.service %{buildroot}/usr/lib/systemd/system/

# CLI symlink
ln -sf /opt/surgewave/cli/surgewave %{buildroot}/usr/bin/surgewave

%files
/opt/surgewave/
/etc/surgewave/
/usr/lib/systemd/system/surgewave-broker.service
/usr/lib/systemd/system/surgewave-control.service
/usr/bin/surgewave
%dir /var/lib/surgewave
%dir /var/log/surgewave

%pre
getent group surgewave >/dev/null || groupadd -r surgewave
getent passwd surgewave >/dev/null || useradd -r -g surgewave -d /var/lib/surgewave -s /sbin/nologin surgewave

%post
chown -R surgewave:surgewave /var/lib/surgewave /var/log/surgewave /opt/surgewave
chmod +x /opt/surgewave/broker/surgewave-broker
chmod +x /opt/surgewave/control/surgewave-control 2>/dev/null || true
chmod +x /opt/surgewave/cli/surgewave 2>/dev/null || true
systemctl daemon-reload
systemctl enable surgewave-broker
echo "Surgewave installed. Start with: sudo systemctl start surgewave-broker"

%preun
systemctl stop surgewave-broker 2>/dev/null || true
systemctl stop surgewave-control 2>/dev/null || true
systemctl disable surgewave-broker 2>/dev/null || true
systemctl disable surgewave-control 2>/dev/null || true

%postun
systemctl daemon-reload
